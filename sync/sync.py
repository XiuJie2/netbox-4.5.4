"""
netbox-pve-sync: 同步 Proxmox VE 到 NetBox
優化版 - 使用預加載和緩存來提高性能，包含 Telegram 通知
"""

import os
import sys
import time
import ipaddress
import hashlib
from typing import Optional, Dict, Any, List, Tuple, Set
import urllib3
import pynetbox
from proxmoxer import ProxmoxAPI, ResourceException
from requests.exceptions import ReadTimeout, ConnectionError
import requests  # 添加 requests 庫


class OptimizedPVEToNetBoxSync:
    """優化的 PVE 到 NetBox 同步器"""
    
    def __init__(self):
        """初始化"""
        self.pve_api = None
        self.nb_api = None
        self.custom_fields_created = False
        
        # Telegram 配置
        self.telegram_bot_token = os.environ['TELEGRAM_BOT_TOKEN']
        self.telegram_chat_id = os.environ['TELEGRAM_CHAT_ID']
        
        # NetBox 對象緩存
        self.nb_cache = {
            'devices': {},              # 按名稱索引的設備
            'virtual_machines': {},     # 按 ID 索引的虛擬機
            'virtual_machines_by_serial': {},  # 按 serial 索引
            'virtual_machines_by_name': {},    # 按名稱和集群索引
            'vm_interfaces': {},        # VM ID -> {接口名: 接口對象}
            'device_interfaces': {},    # 設備 ID -> {接口名: 接口對象}
            'mac_addresses': {},        # MAC地址 -> IP地址對象
            'prefixes': {},             # 網段前綴 -> 網段對象
            'ip_addresses': {},         # IP地址 -> IP對象
            'vlans': {},                # VLAN ID -> VLAN對象
            'vm_disks': {},             # VM ID -> {磁盤名: 磁盤對象}
            'tags': {},                 # 標籤名 -> 標籤對象
            'platforms': {},            # 平台名 -> 平台對象
            'roles': {},                # 角色名 -> 角色對象
            'clusters': {},             # 集群 ID -> 集群對象
            'sites': {},                # 站點名 -> 站點對象
            'manufacturers': {},        # 製造商名 -> 製造商對象
            'device_types': {},         # 設備類型名 -> 設備類型對象
            'device_roles': {},         # 設備角色名 -> 設備角色對象
            'cluster_types': {},        # 集群類型名 -> 集群類型對象
        }
        
        # PVE 緩存
        self.pve_cache = {
            'nodes': [],                # PVE 節點列表
            'vms_by_node': {},          # 節點名 -> VM列表
            'pools': {},                # Pool ID -> Pool信息
            'node_networks': {},        # 節點名 -> 網絡接口列表
        }
        
        # 錯誤記錄
        self.error_log = []
    
    def send_telegram_notification(self, message: str):
        """發送 Telegram 通知"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, data=payload, timeout=10)
            
            if response.status_code == 200:
                print(f"✓ Telegram 通知已發送")
                return True
            else:
                print(f"✗ Telegram 通知發送失敗: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ 發送 Telegram 通知失敗: {e}")
            return False
    
    def log_ip_conflict_error(self, vm_name: str, ip_address: str, error_message: str):
        """記錄 IP 衝突錯誤並發送通知"""
        error_info = {
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'vm_name': vm_name,
            'ip_address': ip_address,
            'error': error_message
        }
        
        self.error_log.append(error_info)
        
        # 創建通知消息
        message = f"""
🚨 <b>PVE-NetBox 同步 IP 衝突警告</b>

📅 時間: {error_info['timestamp']}
🖥️ 虛擬機: {vm_name}
🌐 IP 地址: {ip_address}
❌ 錯誤: {error_message}

⚠️ 需要手動處理
"""
        
        # 發送 Telegram 通知
        self.send_telegram_notification(message)
        
        print(f"📧 已發送 IP 衝突通知: {vm_name} - {ip_address}")
    
    def log_sync_summary(self, success_count: int, total_count: int, error_count: int):
        """發送同步總結通知"""
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
        
        message = f"""
📊 <b>PVE-NetBox 同步完成報告</b>

📅 時間: {time.strftime("%Y-%m-%d %H:%M:%S")}
✅ 成功: {success_count} 個
❌ 失敗: {error_count} 個
📈 總計: {total_count} 個虛擬機
📊 成功率: {success_rate:.1f}%

"""
        
        # 如果有錯誤，添加錯誤詳情
        if self.error_log:
            message += f"⚠️ <b>需要處理的錯誤:</b>\n"
            for error in self.error_log[:5]:  # 最多顯示5個錯誤
                message += f"• {error['vm_name']} - {error['ip_address']}\n"
            
            if len(self.error_log) > 5:
                message += f"• ... 還有 {len(self.error_log) - 5} 個錯誤\n"
        
        self.send_telegram_notification(message)
    
    def connect_pve(self) -> bool:
        """連接 PVE API"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.pve_api = ProxmoxAPI(
                    host=os.environ['PVE_API_HOST'],
                    user=os.environ['PVE_API_USER'],
                    token_name=os.environ['PVE_API_TOKEN'],
                    token_value=os.environ['PVE_API_SECRET'],
                    verify_ssl=os.getenv('PVE_API_VERIFY_SSL', 'false').lower() == 'true',
                    timeout=30,
                )
                
                # 測試連接
                self.pve_api.nodes.get()
                print("✓ PVE API 連接成功")
                return True
                
            except (ReadTimeout, ConnectionError) as e:
                print(f"PVE API 連接失敗 (嘗試 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"等待 {retry_delay} 秒後重試...")
                    time.sleep(retry_delay)
                    
        print("✗ 達到最大重試次數，退出程序")
        return False
    
    def connect_netbox(self) -> bool:
        """連接 NetBox API"""
        try:
            self.nb_api = pynetbox.api(
                url=os.environ['NB_API_URL'],
                token=os.environ['NB_API_TOKEN'],
            )
            self.nb_api.http_session.verify = False
            self.nb_api.http_session.timeout = 30
            
            # 測試連接
            count = self.nb_api.dcim.devices.count()
            print(f"✓ NetBox API 連接成功 (現有設備數: {count})")
            return True
            
        except Exception as e:
            print(f"✗ NetBox API 連接失敗: {e}")
            return False
    
    def load_all_netbox_objects(self):
        """預先加載所有 NetBox 對象到緩存"""
        print("\n預加載 NetBox 對象...")
        start_time = time.time()
        
        # 加載設備
        print("  加載設備...")
        for device in self.nb_api.dcim.devices.all():
            self.nb_cache['devices'][device.name.lower()] = device
        
        # 加載虛擬機
        print("  加載虛擬機...")
        for vm in self.nb_api.virtualization.virtual_machines.all():
            self.nb_cache['virtual_machines'][vm.id] = vm
            
            # 按 serial 索引
            if vm.serial:
                self.nb_cache['virtual_machines_by_serial'][str(vm.serial)] = vm
            
            # 按名稱和集群索引
            cluster_id = vm.cluster.id if vm.cluster else 'no-cluster'
            key = f"{vm.name.lower()}::{cluster_id}"
            self.nb_cache['virtual_machines_by_name'][key] = vm
        
        # 加載虛擬機接口
        print("  加載虛擬機接口...")
        for interface in self.nb_api.virtualization.interfaces.all():
            vm_id = interface.virtual_machine.id
            if vm_id not in self.nb_cache['vm_interfaces']:
                self.nb_cache['vm_interfaces'][vm_id] = {}
            self.nb_cache['vm_interfaces'][vm_id][interface.name] = interface
        
        # 加載設備接口
        print("  加載設備接口...")
        for interface in self.nb_api.dcim.interfaces.all():
            device_id = interface.device.id
            if device_id not in self.nb_cache['device_interfaces']:
                self.nb_cache['device_interfaces'][device_id] = {}
            self.nb_cache['device_interfaces'][device_id][interface.name] = interface
        
        # 加載 MAC 地址
        print("  加載 MAC 地址...")
        for mac in self.nb_api.dcim.mac_addresses.all():
            if mac.mac_address:
                self.nb_cache['mac_addresses'][mac.mac_address.lower()] = mac
        
        # 加載網段
        print("  加載網段...")
        for prefix in self.nb_api.ipam.prefixes.all():
            self.nb_cache['prefixes'][prefix.prefix] = prefix
        
        # 加載 IP 地址
        print("  加載 IP 地址...")
        for ip_addr in self.nb_api.ipam.ip_addresses.all():
            self.nb_cache['ip_addresses'][ip_addr.address] = ip_addr
        
        # 加載 VLAN
        print("  加載 VLAN...")
        for vlan in self.nb_api.ipam.vlans.all():
            self.nb_cache['vlans'][str(vlan.vid)] = vlan
        
        # 加載虛擬磁盤
        print("  加載虛擬磁盤...")
        for disk in self.nb_api.virtualization.virtual_disks.all():
            vm_id = disk.virtual_machine.id
            if vm_id not in self.nb_cache['vm_disks']:
                self.nb_cache['vm_disks'][vm_id] = {}
            self.nb_cache['vm_disks'][vm_id][disk.name] = disk
        
        # 加載標籤
        print("  加載標籤...")
        for tag in self.nb_api.extras.tags.all():
            self.nb_cache['tags'][tag.name] = tag
        
        # 加載平台
        print("  加載平台...")
        for platform in self.nb_api.dcim.platforms.all():
            self.nb_cache['platforms'][platform.name.lower()] = platform
        
        # 加載角色
        print("  加載角色...")
        for role in self.nb_api.dcim.device_roles.all():
            self.nb_cache['roles'][role.name.lower()] = role
        
        # 加載集群
        print("  加載集群...")
        for cluster in self.nb_api.virtualization.clusters.all():
            self.nb_cache['clusters'][str(cluster.id)] = cluster
        
        # 加載站點
        print("  加載站點...")
        for site in self.nb_api.dcim.sites.all():
            self.nb_cache['sites'][site.name.lower()] = site
        
        # 加載製造商
        print("  加載製造商...")
        for manufacturer in self.nb_api.dcim.manufacturers.all():
            self.nb_cache['manufacturers'][manufacturer.name.lower()] = manufacturer
        
        # 加載設備類型
        print("  加載設備類型...")
        for device_type in self.nb_api.dcim.device_types.all():
            self.nb_cache['device_types'][device_type.model.lower()] = device_type
        
        # 加載設備角色
        print("  加載設備角色...")
        for device_role in self.nb_api.dcim.device_roles.all():
            self.nb_cache['device_roles'][device_role.name.lower()] = device_role
        
        # 加載集群類型
        print("  加載集群類型...")
        for cluster_type in self.nb_api.virtualization.cluster_types.all():
            self.nb_cache['cluster_types'][cluster_type.name.lower()] = cluster_type
        
        elapsed = time.time() - start_time
        print(f"✓ 預加載完成，耗時 {elapsed:.2f} 秒")
        print(f"  設備: {len(self.nb_cache['devices'])} 個")
        print(f"  虛擬機: {len(self.nb_cache['virtual_machines'])} 個")
        print(f"  標籤: {len(self.nb_cache['tags'])} 個")
        print(f"  角色: {len(self.nb_cache['roles'])} 個")
    
    def load_pve_data(self):
        """批量加載 PVE 數據到緩存"""
        print("\n加載 PVE 數據...")
        start_time = time.time()
        
        # 加載節點
        print("  加載節點...")
        try:
            self.pve_cache['nodes'] = self.pve_api.nodes.get()
        except Exception as e:
            print(f"  加載節點失敗: {e}")
            self.pve_cache['nodes'] = []
        
        # 加載 Pools
        print("  加載 Pools...")
        try:
            pools = self.pve_api.pools.get()
            for pool in pools:
                pool_id = pool['poolid']
                try:
                    pool_detail = self.pve_api.pools(pool_id).get()
                    members = []
                    for member in pool_detail.get('members', []):
                        if 'vmid' in member:
                            members.append({
                                'vmid': member['vmid'],
                                'type': member.get('type', 'qemu'),
                                'name': member.get('name', '')
                            })
                    
                    self.pve_cache['pools'][pool_id] = {
                        'name': pool_id,
                        'comment': pool.get('comment', ''),
                        'members': members
                    }
                except Exception as e:
                    print(f"    獲取 pool {pool_id} 詳細信息失敗: {e}")
        except Exception as e:
            print(f"  加載 Pools 失敗: {e}")
        
        # 批量加載所有虛擬機
        print("  批量加載虛擬機...")
        for node in self.pve_cache['nodes']:
            node_name = node['node']
            try:
                # 獲取 QEMU 虛擬機
                qemu_vms = self.pve_api.nodes(node_name).qemu.get()
                # 獲取 LXC 容器
                lxc_vms = self.pve_api.nodes(node_name).lxc.get()
                
                # 合併並標記類型
                all_vms = []
                for vm in qemu_vms:
                    vm['type'] = 'qemu'
                    vm['node'] = node_name
                    all_vms.append(vm)
                for vm in lxc_vms:
                    vm['type'] = 'lxc'
                    vm['node'] = node_name
                    all_vms.append(vm)
                
                self.pve_cache['vms_by_node'][node_name] = all_vms
                
                # 預加載 VM 配置（分批處理）
                for vm in all_vms[:10]:  # 先預加載前10個，避免過載
                    try:
                        if vm['type'] == 'qemu':
                            vm['config'] = self.pve_api.nodes(node_name).qemu(vm['vmid']).config.get()
                        else:
                            vm['config'] = self.pve_api.nodes(node_name).lxc(vm['vmid']).config.get()
                    except:
                        pass  # 稍後再獲取配置
                        
            except Exception as e:
                print(f"  加載節點 {node_name} 的虛擬機失敗: {e}")
                self.pve_cache['vms_by_node'][node_name] = []
        
        elapsed = time.time() - start_time
        print(f"✓ PVE 數據加載完成，耗時 {elapsed:.2f} 秒")
        print(f"  節點: {len(self.pve_cache['nodes'])} 個")
        print(f"  Pools: {len(self.pve_cache['pools'])} 個")
        total_vms = sum(len(vms) for vms in self.pve_cache['vms_by_node'].values())
        print(f"  虛擬機: {total_vms} 個")
    
    def check_required_custom_fields(self):
        """檢查必要的 custom fields"""
        print("\n檢查 custom fields...")
        
        try:
            # 獲取現有的 custom fields
            existing_fields = list(self.nb_api.extras.custom_fields.all())
            existing_names = [field.name for field in existing_fields]
            
            # 需要的 custom fields
            required_fields = [
                {
                    'name': 'ha',
                    'label': 'Failover',
                    'type': 'boolean',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'High Availability'
                },
                {
                    'name': 'qemu_agent',
                    'label': 'QemuAgent',
                    'type': 'boolean',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'QEMU Guest Agent Status'
                },
                {
                    'name': 'search_domain',
                    'label': 'Search Domain',
                    'type': 'text',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'Search Domain from Proxmox'
                },
                {
                    'name': 'vm_id',
                    'label': 'VM ID',
                    'type': 'integer',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'Proxmox Virtual Machine ID'
                },
                {
                    'name': 'replicated',
                    'label': 'Replicated',
                    'type': 'boolean',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'VM is replicated'
                },
                {
                    'name': 'machine_type',
                    'label': '機型',
                    'type': 'text',
                    'content_types': ['virtualization.virtualmachine'],
                    'description': 'Proxmox Virtual Machine Type'
                },
            ]
            
            missing_fields = []
            for field in required_fields:
                if field['name'] not in existing_names:
                    missing_fields.append(field)
            
            if missing_fields:
                print(f"✗ 缺少 {len(missing_fields)} 個 custom fields:")
                for field in missing_fields:
                    print(f"  - {field['name']} ({field['type']})")
                
                # 發送 Telegram 通知
                missing_fields_list = "\n".join([f"- {f['name']}" for f in missing_fields])
                notification = f"""
⚠️ <b>NetBox Custom Fields 缺失</b>

以下 custom fields 不存在於 NetBox 中，請創建後重新執行同步：

{missing_fields_list}

NetBox 路徑: Extensions → Custom Fields → Add Custom Field
"""
                self.send_telegram_notification(notification)
                
                print("\n請在 NetBox 中創建這些 custom fields 後重新執行同步。")
                return False
                
            else:
                print("✓ 所有必要的 custom fields 都已存在")
                self.custom_fields_created = True
                return True
                
        except Exception as e:
            print(f"✗ 檢查 custom fields 失敗: {e}")
            return False
    
    def get_vm_pool(self, vm_id: int, vm_type: str) -> Optional[str]:
        """從緩存中獲取 VM 所屬的 Pool"""
        for pool_id, pool_info in self.pve_cache['pools'].items():
            for member in pool_info['members']:
                if member['vmid'] == vm_id and member['type'] == vm_type:
                    return pool_id
        return None
    
    def get_or_create_vm_role(self, role_name: str) -> Optional[int]:
        """獲取或創建虛擬機角色（使用緩存）"""
        if not role_name:
            return None
            
        role_key = role_name.lower()
        
        # 檢查緩存
        if role_key in self.nb_cache['roles']:
            return self.nb_cache['roles'][role_key].id
        
        try:
            # 查找現有角色
            roles = list(self.nb_api.dcim.device_roles.filter(name=role_name))
            if roles:
                role = roles[0]
                self.nb_cache['roles'][role_key] = role
                return role.id
            
            # 創建新角色
            slug = role_name.lower().replace(' ', '-').replace('(', '').replace(')', '').replace('/', '-')[:50]
            
            # 使用 MD5 哈希生成顏色
            color_hash = hashlib.md5(role_name.encode()).hexdigest()[:6]
            
            role = self.nb_api.dcim.device_roles.create(
                name=role_name,
                slug=slug[:50],
                color=color_hash,
                vm_role=True,
                description=f"Proxmox Pool: {role_name}"
            )
            
            self.nb_cache['roles'][role_key] = role
            return role.id
            
        except Exception as e:
            print(f"  創建虛擬機角色失敗 {role_name}: {e}")
            return None
    
    def get_or_create_site(self, site_name: str = "Main Datacenter") -> Optional[int]:
        """獲取或創建站點（使用緩存）"""
        site_key = site_name.lower()
        
        if site_key in self.nb_cache['sites']:
            return self.nb_cache['sites'][site_key].id
        
        try:
            sites = list(self.nb_api.dcim.sites.filter(name=site_name))
            if sites:
                site = sites[0]
                self.nb_cache['sites'][site_key] = site
                return site.id
            
            site = self.nb_api.dcim.sites.create(
                name=site_name,
                slug=site_name.lower().replace(' ', '-'),
                status='active'
            )
            
            self.nb_cache['sites'][site_key] = site
            return site.id
            
        except Exception as e:
            print(f"處理站點失敗: {e}")
            return None
    
    def get_or_create_cluster_type(self, cluster_type_name: str = "Proxmox") -> Optional[int]:
        """獲取或創建集群類型（使用緩存）"""
        cluster_type_key = cluster_type_name.lower()
        
        if cluster_type_key in self.nb_cache['cluster_types']:
            return self.nb_cache['cluster_types'][cluster_type_key].id
        
        try:
            cluster_types = list(self.nb_api.virtualization.cluster_types.filter(name=cluster_type_name))
            if cluster_types:
                cluster_type = cluster_types[0]
                self.nb_cache['cluster_types'][cluster_type_key] = cluster_type
                return cluster_type.id
            
            cluster_type = self.nb_api.virtualization.cluster_types.create(
                name=cluster_type_name,
                slug=cluster_type_name.lower().replace(' ', '-'),
                description="Proxmox Virtual Environment Cluster"
            )
            
            self.nb_cache['cluster_types'][cluster_type_key] = cluster_type
            return cluster_type.id
            
        except Exception as e:
            print(f"處理集群類型失敗: {e}")
            return None
    
    def get_or_create_cluster(self, cluster_name: str, site_id: int, cluster_type_id: int) -> Optional[Dict]:
        """獲取或創建集群"""
        try:
            clusters = list(self.nb_api.virtualization.clusters.filter(name=cluster_name))
            if clusters:
                cluster = clusters[0]
                return {'id': cluster.id, 'name': cluster.name}
            
            cluster = self.nb_api.virtualization.clusters.create(
                name=cluster_name,
                slug=cluster_name.lower().replace(' ', '-'),
                site=site_id,
                type=cluster_type_id
            )
            
            self.nb_cache['clusters'][str(cluster.id)] = cluster
            return {'id': cluster.id, 'name': cluster.name}
            
        except Exception as e:
            print(f"處理集群失敗: {e}")
            return None
    
    def check_qemu_agent(self, config: Dict[str, Any]) -> bool:
        """檢查 QEMU Guest Agent 狀態 - 改進版"""
        if 'agent' in config:
            agent_value = str(config['agent']).strip()
            
            # 移除空格，按逗號分割
            agent_parts = [part.strip() for part in agent_value.split(',')]
            
            # 檢查各種可能的格式
            for part in agent_parts:
                # 檢查 "1" 格式
                if part == '1':
                    return True
                # 檢查 "enabled=1" 格式
                elif part == 'enabled=1':
                    return True
                # 檢查 "fstrim_cloned_disks=1" 但不單獨使用
                elif 'fstrim_cloned_disks=1' in part:
                    return True
        
        return False
    
    def parse_network_config(self, config_value: str) -> Dict:
        """解析網絡配置字符串"""
        result = {}
        for item in config_value.split(','):
            if '=' in item:
                key, value = item.split('=', 1)
                result[key] = value
        return result
    
    def find_existing_vm(self, vm_id: str, vm_name: str, cluster_id: int) -> Optional[Any]:
        """查找現有的虛擬機"""
        # 1. 首先通過 serial (vmid) 查找
        if vm_id in self.nb_cache['virtual_machines_by_serial']:
            return self.nb_cache['virtual_machines_by_serial'][vm_id]
        
        # 2. 通過名稱和集群查找（精確匹配）
        key = f"{vm_name.lower()}::{cluster_id}"
        if key in self.nb_cache['virtual_machines_by_name']:
            return self.nb_cache['virtual_machines_by_name'][key]
        
        # 3. 通過名稱查找（忽略大小寫）
        vm_name_lower = vm_name.lower()
        for key, vm in self.nb_cache['virtual_machines_by_name'].items():
            if key.endswith(f"::{cluster_id}"):
                if vm.name.lower() == vm_name_lower:
                    return vm
                # 檢查是否包含相同的核心名稱
                if vm.name.startswith(vm_name) or vm_name.startswith(vm.name):
                    return vm
        
        return None
    
    def get_vm_agent_network_info(self, node_name: str, vm_id: int, vm_type: str = 'qemu') -> Tuple[Dict[str, List[Dict]], Dict[str, str]]:
        """獲取 VM 的 Agent 網絡信息"""
        try:
            if vm_type == 'qemu':
                interfaces = self.pve_api.nodes(node_name).qemu(vm_id).agent('network-get-interfaces').get()
            else:
                return {}, {}
            
            interface_data = {}
            mac_to_interface = {}
            
            for iface in interfaces.get('result', []):
                if 'name' in iface and 'hardware-address' in iface:
                    iface_name = iface['name']
                    mac_address = iface['hardware-address'].lower()
                    
                    mac_to_interface[mac_address] = iface_name
                    interface_data[iface_name] = iface.get('ip-addresses', [])
            
            return interface_data, mac_to_interface
            
        except ResourceException as e:
            if "is not running" in str(e):
                pass
        except Exception:
            pass
            
        return {}, {}
    
    def process_vm_interfaces(self, vm, vm_config: Dict, agent_interfaces: Dict, mac_to_interface: Dict) -> Tuple[int, Optional[Any]]:
        """處理 VM 網絡接口"""
        interface_count = 0
        primary_ip = None
        
        for config_key, config_value in vm_config.items():
            if not config_key.startswith('net'):
                continue
            
            interface_count += 1
            try:
                config = self.parse_network_config(config_value)
                
                # 提取 MAC 地址
                mac_address = None
                for model in ['virtio', 'e1000', 'vmxnet3', 'rtl8139']:
                    if model in config:
                        mac_address = config[model]
                        break
                
                if not mac_address:
                    continue
                
                # 查找或創建 VM 接口
                vm_interfaces = self.nb_cache['vm_interfaces'].get(vm.id, {})
                if config_key in vm_interfaces:
                    vm_interface = vm_interfaces[config_key]
                    vm_interface.mac_address = mac_address
                    vm_interface.save()
                else:
                    vm_interface = self.nb_api.virtualization.interfaces.create(
                        virtual_machine=vm.id,
                        name=config_key,
                        mac_address=mac_address,
                        enabled=True
                    )
                    if vm.id not in self.nb_cache['vm_interfaces']:
                        self.nb_cache['vm_interfaces'][vm.id] = {}
                    self.nb_cache['vm_interfaces'][vm.id][config_key] = vm_interface
                
                # 查找對應的 Agent 接口
                agent_iface_name = mac_to_interface.get(mac_address.lower())
                
                # 分配 IP 地址
                if agent_iface_name and agent_iface_name in agent_interfaces:
                    ip_addresses = agent_interfaces[agent_iface_name]
                    
                    for ip_info in ip_addresses:
                        if ip_info['ip-address-type'] == 'ipv4' and not ip_info['ip-address'].startswith('127.'):
                            ip_addr = ip_info['ip-address']
                            prefix_len = ip_info['prefix']
                            full_addr = f"{ip_addr}/{prefix_len}"
                            
                            # 分配 IP 到接口
                            ip_obj = self.assign_ip_to_interface(
                                vm_interface, 
                                full_addr, 
                                f"{vm.name}.local", 
                                is_vm_interface=True
                            )
                            if ip_obj and not primary_ip:
                                primary_ip = ip_obj
            
            except Exception as e:
                print(f"    處理接口失敗 {config_key}: {e}")
        
        return interface_count, primary_ip
    
    def assign_ip_to_interface(self, interface, ip_address: str, dns_name: str = None, is_vm_interface: bool = False) -> Optional[Any]:
        """為接口分配 IP 地址，處理 IP 地址衝突並發送通知"""
        try:
            # 解析 IP 地址和網段
            if '/' in ip_address:
                ip_with_prefix = ip_address
            else:
                ip_with_prefix = f"{ip_address}/24"
            
            # 檢查是否已存在
            if ip_with_prefix in self.nb_cache['ip_addresses']:
                ip_obj = self.nb_cache['ip_addresses'][ip_with_prefix]
                ip_obj.assigned_object_type = 'virtualization.vminterface' if is_vm_interface else 'dcim.interface'
                ip_obj.assigned_object_id = interface.id
                if dns_name:
                    ip_obj.dns_name = dns_name
                ip_obj.save()
                return ip_obj
            
            # 創建新 IP
            assigned_object_type = 'virtualization.vminterface' if is_vm_interface else 'dcim.interface'
            
            ip_data = {
                'address': ip_with_prefix,
                'assigned_object_type': assigned_object_type,
                'assigned_object_id': interface.id,
                'status': 'active'
            }
            if dns_name:
                ip_data['dns_name'] = dns_name
            
            ip_obj = self.nb_api.ipam.ip_addresses.create(**ip_data)
            self.nb_cache['ip_addresses'][ip_with_prefix] = ip_obj
            return ip_obj
            
        except Exception as e:
            error_msg = str(e)
            print(f"      分配 IP 地址失敗 {ip_address}: {error_msg}")
            
            # 檢查是否是需要人工處理的錯誤
            if "Cannot reassign IP address while it is designated as the primary IP for the parent object" in error_msg:
                # 獲取虛擬機名稱（如果有的話）
                vm_name = "Unknown"
                try:
                    if is_vm_interface:
                        # 嘗試從接口獲取虛擬機名稱
                        vm_info = self.nb_api.virtualization.interfaces.get(interface.id)
                        if vm_info and vm_info.virtual_machine:
                            vm = vm_info.virtual_machine
                            vm_name = vm.name
                except:
                    pass
                
                # 記錄並發送通知
                self.log_ip_conflict_error(vm_name, ip_address, error_msg)
            
            return None
    
    def get_unique_vm_name(self, vm_name: str, vm_id: str, cluster_id: int) -> str:
        """获取唯一的 VM 名称，如果名称冲突则添加 VM ID 后缀"""
        # 構建緩存鍵
        key = f"{vm_name.lower()}::{cluster_id}"
        
        # 檢查緩存中是否存在這個名稱的虛擬機
        existing_vm = self.nb_cache['virtual_machines_by_name'].get(key)
        
        if existing_vm:
            # 如果找到的虛擬機是同一個（serial相同），則返回原名
            if existing_vm.serial and str(existing_vm.serial) == vm_id:
                return vm_name
            # 如果名稱已存在且不是同一個虛擬機，添加 VM ID 後綴
            unique_name = f"{vm_name}-{vm_id}"
            print(f"  名稱衝突: {vm_name} 已存在，使用新名稱: {unique_name}")
            return unique_name
        
        # 如果緩存中沒有，再檢查是否有其他虛擬機有相同的 serial
        # 這可能發生在虛擬機被重命名但 serial 不變的情況
        for vm in self.nb_cache['virtual_machines_by_serial'].values():
            if vm.serial and str(vm.serial) == vm_id and vm.cluster and vm.cluster.id == cluster_id:
                print(f"  找到相同 serial 的虛擬機: {vm.name} (serial: {vm_id})")
                # 這是同一個虛擬機，但名稱可能已更改
                # 我們應該使用 PVE 中的名稱，但避免名稱衝突
                if vm.name != vm_name:
                    # 如果名稱不同，檢查新名稱是否會衝突
                    new_key = f"{vm_name.lower()}::{cluster_id}"
                    if new_key not in self.nb_cache['virtual_machines_by_name']:
                        return vm_name
                    else:
                        # 新名稱也會衝突，使用帶後綴的名稱
                        unique_name = f"{vm_name}-{vm_id}"
                        print(f"  名稱衝突，使用新名稱: {unique_name}")
                        return unique_name
                return vm_name
        
        # 緩存中沒有同名虛擬機，返回原名
        return vm_name
    
    def process_vm_disks(self, vm, vm_config: Dict) -> Tuple[int, int]:
        """處理 VM 磁盤"""
        disk_count = 0
        disk_size = 0
        
        for config_key, config_value in vm_config.items():
            if config_key.startswith(('scsi', 'virtio', 'sata', 'ide', 'efidisk', 'rootfs')):
                if 'media=cdrom' not in str(config_value):
                    success, size_mb = self.create_virtual_disk(vm, config_key, config_value)
                    if success:
                        disk_count += 1
                        disk_size += size_mb
        
        return disk_count, disk_size
    
    def create_virtual_disk(self, vm, disk_name: str, disk_config: str) -> Tuple[bool, int]:
        """創建虛擬磁盤"""
        try:
            config = self.parse_network_config(disk_config)
            size_str = config.get('size', '0')
            
            # 轉換為 MB
            size_mb = 0
            try:
                if size_str.endswith('G'):
                    size_mb = int(size_str[:-1]) * 1024
                elif size_str.endswith('T'):
                    size_mb = int(size_str[:-1]) * 1024 * 1024
                elif size_str.endswith('M'):
                    size_mb = int(size_str[:-1])
                elif size_str.endswith('K'):
                    size_mb = int(size_str[:-1]) // 1024
                else:
                    size_mb = int(size_str) // (1024 * 1024)
            except (ValueError, AttributeError):
                pass
            
            if size_mb <= 0:
                return False, 0
            
            # 查找或創建磁盤
            vm_disks = self.nb_cache['vm_disks'].get(vm.id, {})
            if disk_name in vm_disks:
                disk = vm_disks[disk_name]
                disk.size = size_mb
                disk.save()
            else:
                disk = self.nb_api.virtualization.virtual_disks.create(
                    virtual_machine=vm.id,
                    name=disk_name,
                    size=size_mb,
                    description=f"Proxmox disk: {disk_name}"
                )
                if vm.id not in self.nb_cache['vm_disks']:
                    self.nb_cache['vm_disks'][vm.id] = {}
                self.nb_cache['vm_disks'][vm.id][disk_name] = disk
            
            return True, size_mb
            
        except Exception as e:
            print(f"    創建虛擬磁盤失敗 {disk_name}: {e}")
            return False, 0
    
    def process_virtual_machine(self, vm_data: Dict, device, cluster: Dict) -> bool:
        """處理單個虛擬機"""
        vm_id = str(vm_data['vmid'])
        original_vm_name = vm_data['name']
        vm_type = vm_data.get('type', 'qemu')
        node_name = vm_data['node']
        
        # 获取唯一的 VM 名称
        vm_name = self.get_unique_vm_name(original_vm_name, vm_id, cluster['id'])
        
        print(f"處理虛擬機: {original_vm_name} (ID: {vm_id}, 類型: {vm_type.upper()}) -> {vm_name}")
        
        # 獲取 VM 配置
        try:
            if 'config' in vm_data:
                vm_config = vm_data['config']
            else:
                if vm_type == 'qemu':
                    vm_config = self.pve_api.nodes(node_name).qemu(vm_data['vmid']).config.get()
                else:
                    vm_config = self.pve_api.nodes(node_name).lxc(vm_data['vmid']).config.get()
        except Exception as e:
            print(f"  獲取 VM 配置失敗: {e}")
            return False
        
        # 檢查是否為模板
        is_template = vm_data.get('is_template', False) or vm_config.get('template', 0) == 1
        
        # --- 定義 boot_choice (vm自啟動) ---
        boot_choice = 'on' if vm_config.get('onboot', 0) == 1 else 'off'


        # 從配置中提取標籤
        tag_ids = []
        if 'tags' in vm_config and vm_config['tags']:
            tag_list = vm_config['tags'].split(';')
            for tag_name in tag_list:
                tag_name = tag_name.strip()
                if tag_name and tag_name in self.nb_cache['tags']:
                    tag_ids.append(self.nb_cache['tags'][tag_name].id)
                elif tag_name:
                    try:
                        slug = tag_name.lower().replace(' ', '-').replace('/', '-')
                        tag = self.nb_api.extras.tags.create(
                            name=tag_name,
                            slug=slug[:50],
                            description=f"Proxmox tag: {tag_name}"
                        )
                        self.nb_cache['tags'][tag_name] = tag
                        tag_ids.append(tag.id)
                    except Exception as e:
                        print(f"  創建標籤失敗 {tag_name}: {e}")
        
        # 獲取 VM 所屬的 Pool，並設置為角色
        role_id = None
        vm_pool = self.get_vm_pool(vm_data['vmid'], vm_type)
        
        if vm_pool:
            role_id = self.get_or_create_vm_role(vm_pool)
            print(f"  使用 PVE Pool 作為角色: {vm_pool}")
        else:
            if vm_type == 'qemu':
                role_id = self.get_or_create_vm_role('Virtual Machine')
            elif vm_type == 'lxc':
                role_id = self.get_or_create_vm_role('Container')
        
        # 獲取 Agent 網絡信息
        agent_interfaces = {}
        mac_to_interface = {}
        
        if vm_type == 'qemu' and not is_template and vm_data.get('status') == 'running':
            agent_interfaces, mac_to_interface = self.get_vm_agent_network_info(node_name, vm_data['vmid'], vm_type)
        
        # 獲取平台
        platform_id = None
        ostype = vm_config.get('ostype')
        if ostype:
            platform_key = ostype.lower()
            if platform_key in self.nb_cache['platforms']:
                platform_id = self.nb_cache['platforms'][platform_key].id
            else:
                try:
                    platform = self.nb_api.dcim.platforms.create(
                        name=ostype,
                        slug=ostype.lower().replace(' ', '-').replace('/', '-')[:50],
                        description=f"Proxmox OS Type: {ostype}"
                    )
                    self.nb_cache['platforms'][platform_key] = platform
                    platform_id = platform.id
                except Exception as e:
                    print(f"  創建平台失敗 {ostype}: {e}")
        
        # 計算 CPU 核心數
        if 'vcpus' in vm_config:
            vcpus = int(vm_config['vcpus'])
        else:
            cores = int(vm_config.get('cores', 1))
            sockets = int(vm_config.get('sockets', 1))
            vcpus = cores * sockets
        
        # 設置狀態
        if is_template:
            status = 'staged'
        elif vm_data.get('status') == 'running':
            status = 'active'
        else:
            status = 'offline'
        
        # 檢查 QEMU Agent
        qemu_agent_enabled = False
        if vm_type == 'qemu' and 'agent' in vm_config:
            qemu_agent_enabled = self.check_qemu_agent(vm_config)
        
        # 準備自定義字段 - 修復 vm_id 衝突
        custom_fields = {}
        if self.custom_fields_created:
            # 檢查是否有其他虛擬機使用了相同的 vm_id
            conflicting_vms = []
            for nb_vm in self.nb_cache['virtual_machines'].values():
                if hasattr(nb_vm, 'custom_fields') and nb_vm.custom_fields:
                    existing_vm_id = nb_vm.custom_fields.get('vm_id')
                    if existing_vm_id == vm_data['vmid']:
                        # 檢查這是否是同一台虛擬機（通過 serial 或名稱判斷）
                        is_same_vm = False
                        if nb_vm.serial and str(nb_vm.serial) == vm_id:
                            is_same_vm = True
                        elif nb_vm.name == vm_name or nb_vm.name == original_vm_name:
                            is_same_vm = True
                        
                        if not is_same_vm:
                            conflicting_vms.append(nb_vm)
            
            if conflicting_vms:
                print(f"  警告: 發現 {len(conflicting_vms)} 個其他虛擬機使用了相同的 vm_id ({vm_id}):")
                for conflict_vm in conflicting_vms:
                    print(f"    - {conflict_vm.name} (ID: {conflict_vm.id})")
                
                # 清除衝突虛擬機的 vm_id，以便我們可以設置正確的值
                for conflict_vm in conflicting_vms:
                    try:
                        # 獲取當前的 custom_fields
                        current_custom_fields = conflict_vm.custom_fields.copy() if conflict_vm.custom_fields else {}
                        # 移除 vm_id
                        if 'vm_id' in current_custom_fields:
                            del current_custom_fields['vm_id']
                            # 更新虛擬機
                            conflict_vm.custom_fields = current_custom_fields
                            conflict_vm.save()
                            print(f"    已從虛擬機 {conflict_vm.name} 中清除 vm_id")
                    except Exception as e:
                        print(f"    清除虛擬機 {conflict_vm.name} 的 vm_id 失敗: {e}")
            
            custom_fields = {
                'vm_id': vm_data['vmid'],
                'qemu_agent': qemu_agent_enabled,
                'ha': False,
                'replicated': False,
                'machine_type': vm_config.get('machine', '')
            }
    
        try:
            # 查找現有虛擬機（优先通过serial查找）
            existing_vm = None
            if vm_id in self.nb_cache['virtual_machines_by_serial']:
                existing_vm = self.nb_cache['virtual_machines_by_serial'][vm_id]
            else:
                # 通过名称和集群查找
                key = f"{vm_name.lower()}::{cluster['id']}"
                if key in self.nb_cache['virtual_machines_by_name']:
                    existing_vm = self.nb_cache['virtual_machines_by_name'][key]
            
            if existing_vm:
                # 更新現有虛擬機
                print(f"  更新現有虛擬機: {existing_vm.name}")
                
                update_data = {
                    'name': vm_name,  # 使用唯一名称
                    'cluster': cluster['id'],
                    'device': device.id,
                    'role': role_id,
                    'vcpus': vcpus,
                    'memory': int(vm_config.get('memory', 0)),
                    'status': status,
                    'description': vm_config.get('description', ''),
                    'platform': platform_id,
                    'start_on_boot': boot_choice
                }
                
                if tag_ids:
                    update_data['tags'] = tag_ids
                
                if custom_fields:
                    update_data['custom_fields'] = custom_fields
                
                existing_vm.update(update_data)
                vm_obj = existing_vm
                
                # 更新缓存中的索引
                if existing_vm.name != vm_name:
                    # 删除旧的名称索引
                    old_key = f"{existing_vm.name.lower()}::{cluster['id']}"
                    if old_key in self.nb_cache['virtual_machines_by_name']:
                        del self.nb_cache['virtual_machines_by_name'][old_key]
                    # 添加新的名称索引
                    new_key = f"{vm_name.lower()}::{cluster['id']}"
                    self.nb_cache['virtual_machines_by_name'][new_key] = existing_vm
            else:
                # 創建新虛擬機
                vm_data_dict = {
                    'serial': vm_id,
                    'name': vm_name,
                    'cluster': cluster['id'],
                    'device': device.id,
                    'role': role_id,
                    'vcpus': vcpus,
                    'memory': int(vm_config.get('memory', 0)),
                    'status': status,
                    'description': vm_config.get('description', ''),
                    'platform': platform_id,
                    'start_on_boot': boot_choice
                }
                
                if tag_ids:
                    vm_data_dict['tags'] = tag_ids
                
                if custom_fields:
                    vm_data_dict['custom_fields'] = custom_fields
                
                vm_obj = self.nb_api.virtualization.virtual_machines.create(**vm_data_dict)
                
                # 更新緩存
                self.nb_cache['virtual_machines'][vm_obj.id] = vm_obj
                self.nb_cache['virtual_machines_by_serial'][vm_id] = vm_obj
                key = f"{vm_name.lower()}::{cluster['id']}"
                self.nb_cache['virtual_machines_by_name'][key] = vm_obj
                
                print(f"  創建虛擬機: {vm_name}")
            
            # 處理網絡接口
            interface_count, primary_ip = self.process_vm_interfaces(vm_obj, vm_config, agent_interfaces, mac_to_interface)
            
            # 處理磁盤
            disk_count, disk_size = self.process_vm_disks(vm_obj, vm_config)
            
            # 更新 VM 的主 IP
            if primary_ip:
                try:
                    vm_obj.primary_ip4 = primary_ip.id
                    vm_obj.save()
                except Exception as e:
                    print(f"  設置 VM 主 IP 失敗: {e}")
            
            # 更新磁盤信息
            #if disk_size > 0:
            #    try:
            #        vm_obj.disk = disk_size
            #        vm_obj.save()
            #    except Exception as e:
            #        print(f"  更新磁盤大小失敗: {e}")
            
            print(f"  標籤: {len(tag_ids)}個, 接口: {interface_count}個, 磁盤: {disk_count}個, 大小: {disk_size}MB")
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"  處理虛擬機失敗: {error_msg}")
            return False
    
    def sync_pve_nodes_to_netbox(self) -> Tuple[bool, Dict[str, Any], Dict]:
        """同步 PVE 節點到 NetBox"""
        print("\n開始同步 PVE 節點...")
        
        # 創建或獲取站點
        site_id = self.get_or_create_site()
        if not site_id:
            print("✗ 無法獲取或創建站點")
            return False, {}, {}
        
        # 創建或獲取集群類型
        cluster_type_id = self.get_or_create_cluster_type()
        if not cluster_type_id:
            print("✗ 無法獲取或創建集群類型")
            return False, {}, {}
        
        # 創建或獲取集群
        cluster = self.get_or_create_cluster("Proxmox Cluster", site_id, cluster_type_id)
        if not cluster:
            print("✗ 無法獲取或創建集群")
            return False, {}, {}
        
        print(f"  使用集群: {cluster['name']} (ID: {cluster['id']})")
        
        # 處理節點
        devices = {}
        success_count = 0
        
        for node in self.pve_cache['nodes']:
            node_name = node['node']
            
            # 查找設備
            device = self.nb_cache['devices'].get(node_name.lower())
            if not device:
                print(f"  ✗ 找不到設備: {node_name}")
                continue
            
            # 更新設備狀態
            try:
                device.status = 'active' if node['status'] == 'online' else 'offline'
                device.save()
            except Exception as e:
                print(f"  更新設備狀態失敗: {e}")
            
            devices[node_name.lower()] = device
            success_count += 1
        
        print(f"  節點同步完成: {success_count}/{len(self.pve_cache['nodes'])} 個節點")
        
        if success_count > 0:
            return True, devices, cluster
        else:
            return False, {}, {}
    
    def sync_pve_virtual_machines(self, devices: Dict[str, Any], cluster: Dict) -> Tuple[bool, int, int]:
        """同步 PVE 虛擬機，返回成功狀態、成功數量和總數量"""
        print("\n開始同步虛擬機...")
        
        success_count = 0
        total_count = 0
        
        for node_name, vms in self.pve_cache['vms_by_node'].items():
            print(f"\n處理節點 {node_name} 的虛擬機:")
            print(f"  發現 {len(vms)} 個虛擬機")
            
            # 獲取對應的設備
            device = devices.get(node_name.lower())
            if not device:
                print(f"  ✗ 找不到對應的設備: {node_name}")
                continue
            
            # 批量處理虛擬機
            for vm in vms:
                total_count += 1
                vm['node'] = node_name  # 確保節點名存在
                if self.process_virtual_machine(vm, device, cluster):
                    success_count += 1
        
        print(f"\n虛擬機同步完成: {success_count}/{total_count} 個虛擬機")
        return success_count > 0, success_count, total_count
    
    def show_summary(self):
        """顯示摘要信息"""
        print("\n" + "="*50)
        print("同步摘要")
        print("="*50)
        
        print(f"PVE 節點: {len(self.pve_cache['nodes'])} 個")
        print(f"PVE Pools: {len(self.pve_cache['pools'])} 個")
        
        total_vms = sum(len(vms) for vms in self.pve_cache['vms_by_node'].values())
        qemu_count = 0
        lxc_count = 0
        
        for node_name, vms in self.pve_cache['vms_by_node'].items():
            for vm in vms:
                if vm.get('type') == 'qemu':
                    qemu_count += 1
                elif vm.get('type') == 'lxc':
                    lxc_count += 1
        
        print(f"PVE 虛擬機: {total_vms} 個 (QEMU: {qemu_count}, LXC: {lxc_count})")
        
        print(f"\nNetBox 緩存統計:")
        print(f"  設備: {len(self.nb_cache['devices'])} 個")
        print(f"  虛擬機: {len(self.nb_cache['virtual_machines'])} 個")
        print(f"  標籤: {len(self.nb_cache['tags'])} 個")
        print(f"  角色: {len(self.nb_cache['roles'])} 個")
        print(f"  IP地址: {len(self.nb_cache['ip_addresses'])} 個")
        print("="*50)
    
    def sync(self):
        """執行同步"""
        print("開始優化的 PVE 到 NetBox 同步")
        print("=" * 50)
        
        # 發送同步開始通知
        start_message = f"""
🔄 <b>PVE-NetBox 同步開始</b>

📅 時間: {time.strftime("%Y-%m-%d %H:%M:%S")}
🚀 開始同步 PVE 到 NetBox
"""
        self.send_telegram_notification(start_message)
        
        start_time = time.time()
        
        # 連接 API
        if not self.connect_pve():
            return
        if not self.connect_netbox():
            return
        
        # 預加載所有 NetBox 對象
        self.load_all_netbox_objects()
        
        # 檢查 custom fields
        if not self.check_required_custom_fields():
            print("\n同步中止。")
            return
        
        # 批量加載 PVE 數據
        self.load_pve_data()
        
        # 顯示摘要
        self.show_summary()
        
        # 同步節點
        nodes_success, devices, cluster = self.sync_pve_nodes_to_netbox()
        
        if nodes_success and devices and cluster:
            print("\n" + "=" * 50)
            print("✓ 節點同步成功")
            
            # 同步虛擬機
            print("\n" + "=" * 50)
            print("開始同步虛擬機...")
            
            vms_success, success_count, total_count = self.sync_pve_virtual_machines(devices, cluster)
            
            elapsed = time.time() - start_time
            error_count = total_count - success_count
            
            print(f"\n同步完成，總耗時: {elapsed:.2f} 秒")
            
            # 發送同步總結通知
            self.log_sync_summary(success_count, total_count, error_count)
            
            if vms_success:
                print("✓ 虛擬機同步成功")
            else:
                print("⚠ 虛擬機同步部分失敗")
        else:
            elapsed = time.time() - start_time
            print(f"\n同步失敗，總耗時: {elapsed:.2f} 秒")
            print("✗ 節點同步失敗")
            
            # 發送失敗通知
            failure_message = f"""
❌ <b>PVE-NetBox 同步失敗</b>

📅 時間: {time.strftime("%Y-%m-%d %H:%M:%S")}
⏱️ 耗時: {elapsed:.2f} 秒
❌ 原因: 節點同步失敗
"""
            self.send_telegram_notification(failure_message)


def main():
    """主函數"""
    # 禁用 SSL 警告
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # 檢查必要環境變量
    required_env_vars = [
        'PVE_API_HOST',
        'PVE_API_USER',
        'PVE_API_TOKEN',
        'PVE_API_SECRET',
        'NB_API_URL',
        'NB_API_TOKEN',
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
    ]
    
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        print(f"✗ 缺少必要環境變量: {', '.join(missing_vars)}")
        print("請設置以下環境變量:")
        for var in required_env_vars:
            print(f"  export {var}=value")
        sys.exit(1)
    
    # 創建同步器並執行
    sync = OptimizedPVEToNetBoxSync()
    sync.sync()


if __name__ == '__main__':
    main()
