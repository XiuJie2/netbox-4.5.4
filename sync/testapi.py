#!/usr/bin/env python3
"""
PVE 和 NetBox 同步工具 - 正式版本
獲取所有 PVE 虛擬機（包括模板和容器）並輸出完整信息
"""

import os
import sys
import json
import urllib3
from datetime import datetime
from typing import Dict, List, Any, Optional
from proxmoxer import ProxmoxAPI
import pynetbox

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置類
class Config:
    """配置管理類"""

    def __init__(self):
        # NetBox 配置
        self.NB_API_URL = os.getenv('NB_API_URL')
        self.NB_API_TOKEN = os.getenv('NB_API_TOKEN')
        self.NB_CLUSTER_ID = os.getenv('NB_CLUSTER_ID', '3')

        # PVE 配置
        self.PVE_API_HOST = os.getenv('PVE_API_HOST')
        self.PVE_API_USER = os.getenv('PVE_API_USER')
        self.PVE_API_TOKEN = os.getenv('PVE_API_TOKEN')
        self.PVE_API_SECRET = os.getenv('PVE_API_SECRET')
        self.PVE_API_VERIFY_SSL = os.getenv('PVE_API_VERIFY_SSL', 'false').lower() == 'true'

        # 驗證必要的配置
        self._validate_config()

    def _validate_config(self):
        """驗證配置是否完整"""
        required_vars = {
            'NB_API_URL': self.NB_API_URL,
            'NB_API_TOKEN': self.NB_API_TOKEN,
            'PVE_API_HOST': self.PVE_API_HOST,
            'PVE_API_USER': self.PVE_API_USER,
            'PVE_API_TOKEN': self.PVE_API_TOKEN,
            'PVE_API_SECRET': self.PVE_API_SECRET,
        }

        missing = [var for var, value in required_vars.items() if not value]
        if missing:
            print(f"錯誤: 缺少必要的環境變量: {', '.join(missing)}")
            print("請設置以下環境變量:")
            for var in missing:
                print(f"  export {var}=your_value")
            sys.exit(1)

# PVE 管理類
class ProxmoxManager:
    """PVE API 管理類"""

    def __init__(self, config: Config):
        self.config = config
        self.pve_api = None
        self._connect()

    def _connect(self):
        """連接到 PVE API"""
        try:
            self.pve_api = ProxmoxAPI(
                host=self.config.PVE_API_HOST,
                user=self.config.PVE_API_USER,
                token_name=self.config.PVE_API_TOKEN,
                token_value=self.config.PVE_API_SECRET,
                verify_ssl=self.config.PVE_API_VERIFY_SSL,
                timeout=30
            )
            print("✓ PVE API 連接成功")
        except Exception as e:
            print(f"✗ PVE API 連接失敗: {e}")
            sys.exit(1)

    def get_all_virtual_machines(self) -> List[Dict[str, Any]]:
        """獲取所有虛擬機（包括模板和容器）"""
        all_vms = []

        try:
            # 獲取所有節點
            nodes = self.pve_api.nodes.get()
            print(f"發現 {len(nodes)} 個節點: {[node['node'] for node in nodes]}")

            for node in nodes:
                node_name = node['node']
                print(f"\n處理節點: {node_name}")

                # 獲取 QEMU/KVM 虛擬機（包括模板）
                try:
                    qemu_vms = self.pve_api.nodes(node_name).qemu.get()
                    print(f"  發現 {len(qemu_vms)} 個 QEMU 虛擬機")

                    for vm in qemu_vms:
                        vm_info = self._get_qemu_vm_details(node_name, vm)
                        all_vms.append(vm_info)

                except Exception as e:
                    print(f"  獲取 QEMU 虛擬機失敗: {e}")

                # 獲取 LXC 容器
                try:
                    lxc_containers = self.pve_api.nodes(node_name).lxc.get()
                    print(f"  發現 {len(lxc_containers)} 個 LXC 容器")

                    for container in lxc_containers:
                        container_info = self._get_lxc_container_details(node_name, container)
                        all_vms.append(container_info)

                except Exception as e:
                    print(f"  獲取 LXC 容器失敗: {e}")

            print(f"\n總計發現 {len(all_vms)} 個虛擬機/容器")
            return all_vms

        except Exception as e:
            print(f"獲取虛擬機列表失敗: {e}")
            return []

    def _get_qemu_vm_details(self, node_name: str, vm: Dict) -> Dict[str, Any]:
        """獲取 QEMU 虛擬機詳細信息"""
        vm_id = vm['vmid']
        vm_details = {
            'type': 'qemu',
            'node': node_name,
            'vmid': vm_id,
            'name': vm.get('name', f'VM-{vm_id}'),
            'status': vm.get('status', 'unknown'),
            'is_template': vm.get('template', 0) == 1,
            'config': {},
            'network_interfaces': [],
            'agent_network_info': None,
            'resources': {},
            'snapshots': [],
        }

        try:
            # 獲取詳細配置
            vm_config = self.pve_api.nodes(node_name).qemu(vm_id).config.get()
            vm_details['config'] = vm_config

            # 從配置中提取網絡接口
            for key, value in vm_config.items():
                if key.startswith('net'):
                    iface_info = self._parse_network_config(value)
                    vm_details['network_interfaces'].append(iface_info)

            # 嘗試通過 Agent 獲取網絡信息
            try:
                agent_info = self.pve_api.nodes(node_name).qemu(vm_id).agent('network-get-interfaces').get()
                vm_details['agent_network_info'] = agent_info
            except Exception as e:
                vm_details['agent_network_info'] = f"無法獲取 Agent 信息: {e}"

            # 獲取資源使用情況
            try:
                resources = self.pve_api.nodes(node_name).qemu(vm_id).rrddata.get(timeframe='hour')
                if resources:
                    vm_details['resources'] = resources[0] if resources else {}
            except:
                pass

            # 獲取快照
            try:
                snapshots = self.pve_api.nodes(node_name).qemu(vm_id).snapshot.get()
                vm_details['snapshots'] = snapshots
            except:
                pass

        except Exception as e:
            vm_details['error'] = f"獲取詳細信息失敗: {e}"

        return vm_details

    def _get_lxc_container_details(self, node_name: str, container: Dict) -> Dict[str, Any]:
        """獲取 LXC 容器詳細信息"""
        container_id = container['vmid']
        container_details = {
            'type': 'lxc',
            'node': node_name,
            'vmid': container_id,
            'name': container.get('name', f'CT-{container_id}'),
            'status': container.get('status', 'unknown'),
            'is_template': container.get('template', 0) == 1,
            'config': {},
            'network_interfaces': [],
            'resources': {},
            'snapshots': [],
        }

        try:
            # 獲取詳細配置
            container_config = self.pve_api.nodes(node_name).lxc(container_id).config.get()
            container_details['config'] = container_config

            # 從配置中提取網絡接口
            for key, value in container_config.items():
                if key.startswith('net'):
                    iface_info = self._parse_network_config(value, is_lxc=True)
                    container_details['network_interfaces'].append(iface_info)

            # 獲取資源使用情況
            try:
                resources = self.pve_api.nodes(node_name).lxc(container_id).rrddata.get(timeframe='hour')
                if resources:
                    container_details['resources'] = resources[0] if resources else {}
            except:
                pass

            # 獲取快照
            try:
                snapshots = self.pve_api.nodes(node_name).lxc(container_id).snapshot.get()
                container_details['snapshots'] = snapshots
            except:
                pass

        except Exception as e:
            container_details['error'] = f"獲取詳細信息失敗: {e}"

        return container_details

    def _parse_network_config(self, config_str: str, is_lxc: bool = False) -> Dict[str, Any]:
        """解析網絡配置字符串"""
        iface_info = {'raw_config': config_str}

        try:
            # 解析類似 "virtio=BC:24:11:1C:44:11,bridge=vmbr0,tag=100" 的格式
            parts = config_str.split(',')
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    iface_info[key] = value

            # 提取 MAC 地址和橋接信息
            if 'virtio' in iface_info:
                iface_info['model'] = 'virtio'
                iface_info['mac_address'] = iface_info['virtio']
            elif 'e1000' in iface_info:
                iface_info['model'] = 'e1000'
                iface_info['mac_address'] = iface_info['e1000']
            elif is_lxc and 'hwaddr' in iface_info:
                iface_info['mac_address'] = iface_info['hwaddr']

        except Exception as e:
            iface_info['parse_error'] = str(e)

        return iface_info

# NetBox 管理類
class NetBoxManager:
    """NetBox API 管理類"""

    def __init__(self, config: Config):
        self.config = config
        self.nb_api = None
        self._connect()

    def _connect(self):
        """連接到 NetBox API"""
        try:
            self.nb_api = pynetbox.api(
                url=self.config.NB_API_URL,
                token=self.config.NB_API_TOKEN,
            )
            self.nb_api.http_session.verify = False
            print("✓ NetBox API 連接成功")
        except Exception as e:
            print(f"✗ NetBox API 連接失敗: {e}")
            sys.exit(1)

    def get_all_virtual_machines(self) -> List[Dict[str, Any]]:
        """獲取 NetBox 中的所有虛擬機"""
        try:
            # 獲取所有虛擬機
            nb_vms = list(self.nb_api.virtualization.virtual_machines.all())
            print(f"NetBox 中有 {len(nb_vms)} 個虛擬機")

            vm_details = []
            for vm in nb_vms:
                vm_info = {
                    'id': vm.id,
                    'name': vm.name,
                    'status': vm.status.value if vm.status else 'unknown',
                    'serial': vm.serial,
                    'vcpus': vm.vcpus,
                    'memory': vm.memory,
                    'disk': vm.disk,
                    'cluster': str(vm.cluster) if vm.cluster else None,
                    'tenant': str(vm.tenant) if vm.tenant else None,
                    'platform': str(vm.platform) if vm.platform else None,
                    'interfaces': [],
                }

                # 獲取接口信息
                interfaces = self.nb_api.virtualization.interfaces.filter(virtual_machine_id=vm.id)
                for interface in interfaces:
                    iface_info = {
                        'id': interface.id,
                        'name': interface.name,
                        'type': interface.type.value if interface.type else 'virtual',
                        'mac_address': interface.mac_address,
                        'mtu': interface.mtu,
                        'enabled': interface.enabled,
                        'ip_addresses': [],
                    }

                    # 獲取 IP 地址
                    ip_addresses = self.nb_api.ipam.ip_addresses.filter(interface_id=interface.id)
                    for ip_addr in ip_addresses:
                        iface_info['ip_addresses'].append({
                            'id': ip_addr.id,
                            'address': ip_addr.address,
                            'status': ip_addr.status.value if ip_addr.status else 'active',
                            'dns_name': ip_addr.dns_name,
                        })

                    vm_info['interfaces'].append(iface_info)

                vm_details.append(vm_info)

            return vm_details

        except Exception as e:
            print(f"獲取 NetBox 虛擬機失敗: {e}")
            return []

    def get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """獲取集群信息"""
        try:
            cluster = self.nb_api.virtualization.clusters.get(self.config.NB_CLUSTER_ID)
            if cluster:
                return {
                    'id': cluster.id,
                    'name': cluster.name,
                    'type': str(cluster.type) if cluster.type else None,
                    'site': str(cluster.site) if cluster.site else None,
                }
        except Exception as e:
            print(f"獲取集群信息失敗: {e}")
        return None

# 主程序
class PVEToNetBoxSync:
    """PVE 到 NetBox 同步主程序"""

    def __init__(self):
        self.config = Config()
        self.pve_manager = ProxmoxManager(self.config)
        self.netbox_manager = NetBoxManager(self.config)

    def run(self):
        """運行主程序"""
        print("=" * 80)
        print(f"PVE 到 NetBox 同步工具 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        # 1. 獲取 PVE 所有虛擬機
        print("\n1. 從 PVE 獲取虛擬機信息...")
        pve_vms = self.pve_manager.get_all_virtual_machines()

        # 2. 獲取 NetBox 所有虛擬機
        print("\n2. 從 NetBox 獲取虛擬機信息...")
        nb_vms = self.netbox_manager.get_all_virtual_machines()

        # 3. 獲取 NetBox 集群信息
        print("\n3. 獲取 NetBox 集群信息...")
        cluster_info = self.netbox_manager.get_cluster_info()

        # 4. 輸出結果
        self._output_results(pve_vms, nb_vms, cluster_info)

        # 5. 生成統計報告
        self._generate_statistics(pve_vms, nb_vms)

    def _output_results(self, pve_vms: List[Dict], nb_vms: List[Dict], cluster_info: Optional[Dict]):
        """輸出結果到文件和屏幕"""

        # 創建輸出目錄
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. 輸出 PVE 虛擬機信息
        pve_output_file = os.path.join(output_dir, f"pve_vms_{timestamp}.json")
        with open(pve_output_file, 'w', encoding='utf-8') as f:
            json.dump(pve_vms, f, indent=2, ensure_ascii=False, default=str)

        print(f"\n✓ PVE 虛擬機信息已保存到: {pve_output_file}")

        # 2. 輸出 NetBox 虛擬機信息
        nb_output_file = os.path.join(output_dir, f"netbox_vms_{timestamp}.json")
        with open(nb_output_file, 'w', encoding='utf-8') as f:
            json.dump(nb_vms, f, indent=2, ensure_ascii=False, default=str)

        print(f"✓ NetBox 虛擬機信息已保存到: {nb_output_file}")

        # 3. 輸出摘要報告
        summary_file = os.path.join(output_dir, f"summary_{timestamp}.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write(f"PVE 到 NetBox 同步摘要報告\n")
            f.write(f"生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

            # 集群信息
            if cluster_info:
                f.write("NetBox 集群信息:\n")
                f.write(f"  名稱: {cluster_info.get('name', 'N/A')}\n")
                f.write(f"  ID: {cluster_info.get('id', 'N/A')}\n")
                f.write(f"  類型: {cluster_info.get('type', 'N/A')}\n")
                f.write(f"  站點: {cluster_info.get('site', 'N/A')}\n\n")

            # PVE 統計
            f.write("PVE 統計:\n")
            total_pve = len(pve_vms)
            qemu_vms = [vm for vm in pve_vms if vm['type'] == 'qemu']
            lxc_vms = [vm for vm in pve_vms if vm['type'] == 'lxc']
            templates = [vm for vm in pve_vms if vm.get('is_template', False)]

            f.write(f"  總虛擬機/容器數: {total_pve}\n")
            f.write(f"  QEMU 虛擬機: {len(qemu_vms)}\n")
            f.write(f"  LXC 容器: {len(lxc_vms)}\n")
            f.write(f"  模板數量: {len(templates)}\n\n")

            # NetBox 統計
            f.write("NetBox 統計:\n")
            f.write(f"  總虛擬機數: {len(nb_vms)}\n")

            # 狀態統計
            status_count = {}
            for vm in nb_vms:
                status = vm.get('status', 'unknown')
                status_count[status] = status_count.get(status, 0) + 1

            if status_count:
                f.write("  狀態分佈:\n")
                for status, count in status_count.items():
                    f.write(f"    {status}: {count}\n")

        print(f"✓ 摘要報告已保存到: {summary_file}")

        # 4. 在屏幕上顯示摘要
        print("\n" + "=" * 80)
        print("摘要報告:")
        print("-" * 80)

        if cluster_info:
            print(f"NetBox 集群: {cluster_info.get('name')} (ID: {cluster_info.get('id')})")

        print(f"\nPVE 發現:")
        print(f"  總數: {len(pve_vms)} 個虛擬機/容器")

        qemu_vms = [vm for vm in pve_vms if vm['type'] == 'qemu']
        lxc_vms = [vm for vm in pve_vms if vm['type'] == 'lxc']
        templates = [vm for vm in pve_vms if vm.get('is_template', False)]

        print(f"  - QEMU 虛擬機: {len(qemu_vms)}")
        print(f"  - LXC 容器: {len(lxc_vms)}")
        print(f"  - 模板: {len(templates)}")

        print(f"\nNetBox 發現:")
        print(f"  總數: {len(nb_vms)} 個虛擬機")

        # 顯示前幾個 PVE VM 作為示例
        print(f"\nPVE 虛擬機示例 (前5個):")
        for i, vm in enumerate(pve_vms[:5]):
            template_mark = "[模板]" if vm.get('is_template') else ""
            print(f"  {i+1}. {vm['name']} (ID: {vm['vmid']}, 類型: {vm['type']}) {template_mark}")

    def _generate_statistics(self, pve_vms: List[Dict], nb_vms: List[Dict]):
        """生成詳細統計信息"""

        # 按節點統計
        node_stats = {}
        for vm in pve_vms:
            node = vm.get('node', 'unknown')
            if node not in node_stats:
                node_stats[node] = {'qemu': 0, 'lxc': 0, 'templates': 0}

            if vm['type'] == 'qemu':
                node_stats[node]['qemu'] += 1
            else:
                node_stats[node]['lxc'] += 1

            if vm.get('is_template'):
                node_stats[node]['templates'] += 1

        print("\n" + "=" * 80)
        print("按節點統計:")
        print("-" * 80)
        for node, stats in node_stats.items():
            total = stats['qemu'] + stats['lxc']
            print(f"{node}:")
            print(f"  總數: {total} (QEMU: {stats['qemu']}, LXC: {stats['lxc']})")
            if stats['templates'] > 0:
                print(f"  模板: {stats['templates']}")

        # 狀態統計
        print("\nPVE 虛擬機狀態:")
        status_stats = {}
        for vm in pve_vms:
            status = vm.get('status', 'unknown')
            status_stats[status] = status_stats.get(status, 0) + 1

        for status, count in status_stats.items():
            print(f"  {status}: {count}")

if __name__ == '__main__':
    try:
        sync_tool = PVEToNetBoxSync()
        sync_tool.run()
        print("\n" + "=" * 80)
        print("程序執行完成！")
        print("詳細信息已保存到 output/ 目錄")
        print("=" * 80)
    except KeyboardInterrupt:
        print("\n\n程序被用戶中斷")
        sys.exit(0)
    except Exception as e:
        print(f"\n程序執行錯誤: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)