"""
netbox-pve-sync: 同步 Proxmox VE 到 NetBox
優化版 - 快速同步 + 完整功能
"""

import os
import sys
import time
import ipaddress
from typing import Optional, Dict, Any, List, Tuple, Set
import urllib3
import pynetbox
from proxmoxer import ProxmoxAPI, ResourceException
from requests.exceptions import ReadTimeout, ConnectionError


class PVEToNetBoxSync:
    """PVE 到 NetBox 同步器 - 優化版本"""
    
    def __init__(self):
        """初始化"""
        self.pve_api = None
        self.nb_api = None
        self.custom_fields_created = False
        
        # 擴展緩存對象
        self.nb_objects = {
            'tags': {},
            'platforms': {},
            'prefixes': {},
            'roles': {},
            'devices': {},  # 設備緩存
            'virtual_machines': {},  # 虛擬機緩存
            'vm_by_serial': {},  # 按 serial 索引
            'vm_by_name_cluster': {},  # 按名稱+集群索引
            'vm_interfaces': {},  # VM 接口緩存
            'device_interfaces': {},  # 設備接口緩存
            'ip_addresses': {},  # IP 地址緩存
            'virtual_disks': {},  # 虛擬磁盤緩存
            'clusters': {},  # 集群緩存
            'sites': {},  # 站點緩存
            'cluster_types': {},  # 集群類型緩存
            'manufacturers': {},  # 製造商緩存
            'device_types': {},  # 設備類型緩存
            'device_roles': {},  # 設備角色緩存
        }
    
    def connect_pve(self) -> bool:
        """連接 PVE API - 優化版本"""
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
    
    def load_netbox_objects(self):
        """加載 NetBox 對象到緩存 - 優化版本"""
        print("\n加載 NetBox 對象...")
        
        start_time = time.time()
        
        # 批量加載各種對象
        objects_to_load = [
            ('tags', self.nb_api.extras.tags.all()),
            ('platforms', self.nb_api.dcim.platforms.all()),
            ('prefixes', self.nb_api.ipam.prefixes.all()),
            ('roles', self.nb_api.dcim.device_roles.all()),
            ('devices', self.nb_api.dcim.devices.all()),
            ('sites', self.nb_api.dcim.sites.all()),
            ('cluster_types', self.nb_api.virtualization.cluster_types.all()),
            ('clusters', self.nb_api.virtualization.clusters.all()),
            ('manufacturers', self.nb_api.dcim.manufacturers.all()),
            ('device_types', self.nb_api.dcim.device_types.all()),
            ('device_roles', self.nb_api.dcim.device_roles.all()),
        ]
        
        for obj_name, obj_query in objects_to_load:
            try:
                count = 0
                for obj in obj_query:
                    if obj_name == 'tags':
                        self.nb_objects['tags'][obj.name] = obj
                    elif obj_name == 'platforms':
                        self.nb_objects['platforms'][obj.name.lower()] = obj
                    elif obj_name == 'prefixes':
                        self.nb_objects['prefixes'][obj.prefix] = obj
                    elif obj_name == 'roles':
                        self.nb_objects['roles'][obj.name.lower()] = obj
                    elif obj_name == 'devices':
                        self.nb_objects['devices'][obj.name.lower()] = obj
                    elif obj_name == 'sites':
                        self.nb_objects['sites'][obj.name.lower()] = obj
                    elif obj_name == 'cluster_types':
                        self.nb_objects['cluster_types'][obj.name.lower()] = obj
                    elif obj_name == 'clusters':
                        self.nb_objects['clusters'][obj.name.lower()] = obj
                    elif obj_name == 'manufacturers':
                        self.nb_objects['manufacturers'][obj.name.lower()] = obj
                    elif obj_name == 'device_types':
                        self.nb_objects['device_types'][obj.model.lower()] = obj
                    elif obj_name == 'device_roles':
                        self.nb_objects['device_roles'][obj.name.lower()] = obj
                    count += 1
                print(f"  ✓ 加載 {count} 個 {obj_name}")
            except Exception as e:
                print(f"  ✗ 加載 {obj_name} 失敗: {e}")
        
        # 加載虛擬機及其相關對象
        self._load_virtual_machines()
        self._load_interfaces_and_ips()
        self._load_virtual_disks()
        
        elapsed_time = time.time() - start_time
        print(f"✓ 對象加載完成，耗時: {elapsed_time:.2f}秒")
    
    def _load_virtual_machines(self):
        """加載虛擬機到緩存"""
        try:
            vms = list(self.nb_api.virtualization.virtual_machines.all())
            for vm in vms:
                # 存儲到不同索引
                self.nb_objects['virtual_machines'][vm.id] = vm
                
                # 按 serial 索引
                if vm.serial:
                    self.nb_objects['vm_by_serial'][str(vm.serial)] = vm
                
                # 按名稱+集群索引
                cluster_id = vm.cluster.id if vm.cluster else 'no-cluster'
                key = f"{vm.name}::{cluster_id}"
                self.nb_objects['vm_by_name_cluster'][key] = vm
            
            print(f"  ✓ 加載 {len(vms)} 個虛擬機")
        except Exception as e:
            print(f"  ✗ 加載虛擬機失敗: {e}")
    
    def _load_interfaces_and_ips(self):
        """加載接口和 IP 地址到緩存"""
        try:
            # 加載設備接口
            device_interfaces = list(self.nb_api.dcim.interfaces.all())
            for iface in device_interfaces:
                if iface.device.id not in self.nb_objects['device_interfaces']:
                    self.nb_objects['device_interfaces'][iface.device.id] = {}
                self.nb_objects['device_interfaces'][iface.device.id][iface.name] = iface
            
            # 加載 VM 接口
            vm_interfaces = list(self.nb_api.virtualization.interfaces.all())
            for iface in vm_interfaces:
                if iface.virtual_machine.id not in self.nb_objects['vm_interfaces']:
                    self.nb_objects['vm_interfaces'][iface.virtual_machine.id] = {}
                self.nb_objects['vm_interfaces'][iface.virtual_machine.id][iface.name] = iface
            
            # 加載 IP 地址
            ip_addresses = list(self.nb_api.ipam.ip_addresses.all())
            for ip in ip_addresses:
                self.nb_objects['ip_addresses'][ip.address] = ip
            
            print(f"  ✓ 加載 {len(device_interfaces)} 個設備接口, {len(vm_interfaces)} 個 VM 接口, {len(ip_addresses)} 個 IP 地址")
        except Exception as e:
            print(f"  ✗ 加載接口和 IP 失敗: {e}")
    
    def _load_virtual_disks(self):
        """加載虛擬磁盤到緩存"""
        try:
            disks = list(self.nb_api.virtualization.virtual_disks.all())
            for disk in disks:
                if disk.virtual_machine.id not in self.nb_objects['virtual_disks']:
                    self.nb_objects['virtual_disks'][disk.virtual_machine.id] = {}
                self.nb_objects['virtual_disks'][disk.virtual_machine.id][disk.name] = disk
            
            print(f"  ✓ 加載 {len(disks)} 個虛擬磁盤")
        except Exception as e:
            print(f"  ✗ 加載虛擬磁盤失敗: {e}")
    
    def check_required_custom_fields(self):
        """檢查必要的 custom fields"""
        print("\n檢查 custom fields...")
        
        try:
            existing_fields = list(self.nb_api.extras.custom_fields.all())
            existing_names = {field.name for field in existing_fields}
            
            required_fields = [
                {'name': 'ha', 'type': 'boolean'},
                {'name': 'qemu_agent', 'type': 'boolean'},
                {'name': 'search_domain', 'type': 'text'},
                {'name': 'autostart', 'type': 'boolean'},
                {'name': 'vm_id', 'type': 'integer'},
                {'name': 'replicated', 'type': 'boolean'},
                {'name': 'pool', 'type': 'text'},  # 新增 pool 字段
            ]
            
            missing_fields = [field for field in required_fields if field['name'] not in existing_names]
            
            if missing_fields:
                print("✗ 缺少以下 custom fields:")
                for field in missing_fields:
                    print(f"  - {field['name']} ({field['type']})")
                
                print("\n請在 NetBox 中創建這些字段，應用於: Virtual Machine")
                return False
                
            print("✓ 所有必要的 custom fields 都已存在")
            self.custom_fields_created = True
            return True
            
        except Exception as e:
            print(f"✗ 檢查 custom fields 失敗: {e}")
            return False
    
    def find_existing_vm(self, vm_id: str, vm_name: str, cluster_id: int) -> Optional[Any]:
        """查找現有的虛擬機 - 使用緩存"""
        # 1. 首先通過 serial (vmid) 查找
        if vm_id in self.nb_objects['vm_by_serial']:
            vm = self.nb_objects['vm_by_serial'][vm_id]
            print(f"  通過 serial 找到現有虛擬機: {vm.name}")
            return vm
        
        # 2. 通過名稱和集群查找
        key = f"{vm_name}::{cluster_id}"
        if key in self.nb_objects['vm_by_name_cluster']:
            vm = self.nb_objects['vm_by_name_cluster'][key]
            print(f"  通過名稱+集群找到現有虛擬機: {vm.name}")
            return vm
        
        # 3. 通過名稱查找（忽略大小寫）
        vm_name_lower = vm_name.lower()
        for vm in self.nb_objects['virtual_machines'].values():
            if vm.cluster and vm.cluster.id == cluster_id:
                if vm.name.lower() == vm_name_lower:
                    print(f"  通過名稱（忽略大小寫）找到現有虛擬機: {vm.name}")
                    return vm
        
        return None
    
    def get_vm_config(self, node_name: str, vm_id: int, vm_type: str = 'qemu') -> Optional[Dict]:
        """獲取 VM 配置 - 優化版本"""
        try:
            if vm_type == 'qemu':
                return self.pve_api.nodes(node_name).qemu(vm_id).config.get()
            else:  # lxc
                return self.pve_api.nodes(node_name).lxc(vm_id).config.get()
        except Exception as e:
            print(f"  獲取 VM {vm_id} 配置失敗: {e}")
            return None
    
    def get_vm_agent_network_info(self, node_name: str, vm_id: int, vm_type: str = 'qemu') -> Tuple[Dict, Dict]:
        """獲取 VM 的 Agent 網絡信息 - 優化版本"""
        if vm_type != 'qemu':
            return {}, {}
        
        try:
            interfaces = self.pve_api.nodes(node_name).qemu(vm_id).agent('network-get-interfaces').get()
            
            interface_data = {}
            mac_to_interface = {}
            
            for iface in interfaces.get('result', []):
                if 'name' in iface and 'hardware-address' in iface:
                    iface_name = iface['name']
                    mac_address = iface['hardware-address'].lower()
                    
                    mac_to_interface[mac_address] = iface_name
                    interface_data[iface_name] = iface.get('ip-addresses', [])
            
            return interface_data, mac_to_interface
            
        except ResourceException:
            return {}, {}  # 虛擬機未運行或無 Agent
        except Exception:
            return {}, {}
    
    def extract_vm_tags(self, config: Dict[str, Any]) -> List[str]:
        """從 VM 配置中提取標籤"""
        tags = []
        if 'tags' in config and config['tags']:
            tag_list = config['tags'].split(';')
            tags = [tag.strip() for tag in tag_list if tag.strip()]
        return tags
    
    def check_qemu_agent(self, config: Dict[str, Any]) -> bool:
        """檢查 QEMU Guest Agent 狀態"""
        if 'agent' in config:
            agent_value = str(config['agent']).strip()
            if agent_value == '1' or 'enabled=1' in agent_value:
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
    
    def get_or_create_vm_role(self, role_name: str) -> Optional[int]:
        """獲取或創建虛擬機角色 - 使用緩存"""
        role_key = role_name.lower()
        
        if role_key in self.nb_objects['roles']:
            return self.nb_objects['roles'][role_key].id
        
        try:
            roles = list(self.nb_api.dcim.device_roles.filter(name=role_name))
            if roles:
                role = roles[0]
                self.nb_objects['roles'][role_key] = role
                return role.id
            
            # 創建新角色
            slug = role_name.lower().replace(' ', '-').replace('(', '').replace(')', '')
            color_map = {
                'virtual machine (qemu)': '00ff00',
                'container (lxc)': '0000ff',
            }
            color = color_map.get(role_key, '808080')
            
            role = self.nb_api.dcim.device_roles.create(
                name=role_name,
                slug=slug[:50],
                color=color,
                vm_role=True,
                description=f"{role_name} role for Proxmox"
            )
            
            self.nb_objects['roles'][role_key] = role
            print(f"  創建虛擬機角色: {role_name}")
            return role.id
            
        except Exception as e:
            print(f"  創建虛擬機角色失敗 {role_name}: {e}")
            return None
    
    def get_or_create_tag(self, tag_name: str) -> Optional[int]:
        """獲取或創建標籤 - 使用緩存"""
        if not tag_name or tag_name.strip() == '':
            return None
        
        tag_name = tag_name.strip()
        
        if tag_name in self.nb_objects['tags']:
            return self.nb_objects['tags'][tag_name].id
        
        try:
            slug = tag_name.lower().replace(' ', '-').replace('/', '-').replace('_', '-')[:50]
            tag = self.nb_api.extras.tags.create(
                name=tag_name,
                slug=slug,
                description=f"Proxmox tag: {tag_name}"
            )
            
            self.nb_objects['tags'][tag_name] = tag
            return tag.id
            
        except Exception as e:
            print(f"  創建標籤失敗 {tag_name}: {e}")
            return None
    
    def get_or_create_platform(self, ostype: str) -> Optional[int]:
        """獲取或創建平台 - 使用緩存"""
        if not ostype:
            return None
        
        platform_key = ostype.lower()
        
        if platform_key in self.nb_objects['platforms']:
            return self.nb_objects['platforms'][platform_key].id
        
        try:
            platform = self.nb_api.dcim.platforms.create(
                name=ostype,
                slug=ostype.lower().replace(' ', '-').replace('/', '-')[:50],
                description=f"Proxmox OS Type: {ostype}"
            )
            
            self.nb_objects['platforms'][platform_key] = platform
            return platform.id
            
        except Exception as e:
            print(f"  創建平台失敗 {ostype}: {e}")
            return None
    
    def process_vm_interface(self, vm, interface_name: str, interface_config: str, 
                           agent_interfaces: Dict, mac_to_interface: Dict) -> Optional[Any]:
        """處理 VM 接口 - 優化版本"""
        try:
            config = self.parse_network_config(interface_config)
            
            # 提取 MAC 地址
            mac_address = None
            for model in ['virtio', 'e1000', 'vmxnet3', 'rtl8139']:
                if model in config:
                    mac_address = config[model]
                    break
            
            if not mac_address:
                return None
            
            # 檢查緩存
            vm_interfaces = self.nb_objects['vm_interfaces'].get(vm.id, {})
            vm_interface = vm_interfaces.get(interface_name)
            
            if vm_interface:
                # 更新現有接口
                if vm_interface.mac_address != mac_address:
                    vm_interface.mac_address = mac_address
                    vm_interface.save()
            else:
                # 創建新接口
                vm_interface = self.nb_api.virtualization.interfaces.create(
                    virtual_machine=vm.id,
                    name=interface_name,
                    mac_address=mac_address,
                    enabled=True
                )
                # 更新緩存
                if vm.id not in self.nb_objects['vm_interfaces']:
                    self.nb_objects['vm_interfaces'][vm.id] = {}
                self.nb_objects['vm_interfaces'][vm.id][interface_name] = vm_interface
            
            # 分配 IP 地址
            self.assign_ip_to_vm_interface(vm, vm_interface, mac_address, agent_interfaces, mac_to_interface)
            
            return vm_interface
            
        except Exception as e:
            print(f"    處理 VM 接口失敗 {interface_name}: {e}")
            return None
    
    def assign_ip_to_vm_interface(self, vm, interface, mac_address: str, 
                                 agent_interfaces: Dict, mac_to_interface: Dict):
        """為 VM 接口分配 IP 地址 - 優化版本"""
        agent_iface_name = mac_to_interface.get(mac_address.lower())
        
        if agent_iface_name and agent_iface_name in agent_interfaces:
            ip_addresses = agent_interfaces[agent_iface_name]
            
            for ip_info in ip_addresses:
                if ip_info['ip-address-type'] == 'ipv4' and not ip_info['ip-address'].startswith('127.'):
                    ip_addr = ip_info['ip-address']
                    prefix_len = ip_info['prefix']
                    full_addr = f"{ip_addr}/{prefix_len}"
                    
                    # 檢查 IP 是否已存在
                    if full_addr in self.nb_objects['ip_addresses']:
                        ip_obj = self.nb_objects['ip_addresses'][full_addr]
                        ip_obj.assigned_object_type = 'virtualization.vminterface'
                        ip_obj.assigned_object_id = interface.id
                        ip_obj.save()
                    else:
                        ip_obj = self.nb_api.ipam.ip_addresses.create(
                            address=full_addr,
                            assigned_object_type='virtualization.vminterface',
                            assigned_object_id=interface.id,
                            status='active',
                            dns_name=f"{vm.name}.local"
                        )
                        self.nb_objects['ip_addresses'][full_addr] = ip_obj
                    
                    # 設置為主 IP
                    if not vm.primary_ip4:
                        try:
                            vm.primary_ip4 = ip_obj.id
                            vm.save()
                        except Exception:
                            pass
    
    def process_virtual_disk(self, vm, disk_name: str, disk_config: str) -> Tuple[bool, int]:
        """處理虛擬磁盤 - 優化版本"""
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
            
            # 檢查緩存
            vm_disks = self.nb_objects['virtual_disks'].get(vm.id, {})
            disk = vm_disks.get(disk_name)
            
            if disk:
                # 更新現有磁盤
                disk.size = size_mb
                disk.save()
            else:
                # 創建新磁盤
                disk = self.nb_api.virtualization.virtual_disks.create(
                    virtual_machine=vm.id,
                    name=disk_name,
                    size=size_mb,
                    description=f"Proxmox disk: {disk_name}"
                )
                # 更新緩存
                if vm.id not in self.nb_objects['virtual_disks']:
                    self.nb_objects['virtual_disks'][vm.id] = {}
                self.nb_objects['virtual_disks'][vm.id][disk_name] = disk
            
            return True, size_mb
            
        except Exception as e:
            print(f"    處理虛擬磁盤失敗 {disk_name}: {e}")
            return False, 0
    
    def create_or_update_virtual_machine(self, node_name: str, vm_data: Dict[str, Any], 
                                       device, cluster: Dict) -> bool:
        """創建或更新虛擬機 - 優化版本"""
        vm_id = str(vm_data['vmid'])
        original_vm_name = vm_data['name']
        vm_type = vm_data.get('type', 'qemu')
        
        # 查找現有虛擬機
        existing_vm = self.find_existing_vm(vm_id, original_vm_name, cluster['id'])
        
        if existing_vm:
            vm_name = existing_vm.name
            print(f"\n處理虛擬機: {original_vm_name} (ID: {vm_id}, 類型: {vm_type.upper()}) -> 已存在: {vm_name}")
        else:
            vm_name = original_vm_name
            print(f"\n處理虛擬機: {original_vm_name} (ID: {vm_id}, 類型: {vm_type.upper()}) -> {vm_name}")
        
        # 獲取 VM 配置
        vm_config = self.get_vm_config(node_name, vm_data['vmid'], vm_type)
        if not vm_config:
            return False
        
        # 檢查是否為模板
        is_template = vm_data.get('is_template', False) or vm_config.get('template', 0) == 1
        
        # 提取標籤
        tag_ids = []
        pve_tags = self.extract_vm_tags(vm_config)
        for tag_name in pve_tags:
            tag_id = self.get_or_create_tag(tag_name)
            if tag_id:
                tag_ids.append(tag_id)
        
        # 獲取 Agent 網絡信息
        agent_interfaces, mac_to_interface = {}, {}
        if vm_type == 'qemu' and not is_template and vm_data.get('status') == 'running':
            agent_interfaces, mac_to_interface = self.get_vm_agent_network_info(node_name, vm_data['vmid'], vm_type)
        
        # 獲取平台
        platform_id = self.get_or_create_platform(vm_config.get('ostype'))
        
        # 設置虛擬機角色
        role_id = None
        if vm_type == 'qemu':
            role_id = self.get_or_create_vm_role('Virtual Machine (QEMU)')
        elif vm_type == 'lxc':
            role_id = self.get_or_create_vm_role('Container (LXC)')
        
        # 準備自定義字段
        custom_fields = {}
        if self.custom_fields_created:
            custom_fields = {
                'vm_id': vm_data['vmid'],
                'qemu_agent': self.check_qemu_agent(vm_config) if vm_type == 'qemu' else False,
                'autostart': vm_config.get('onboot', 0) == 1,
                'ha': False,
                'replicated': False,
            }
            # 添加 pool 信息
            if 'pool' in vm_config and vm_config['pool']:
                custom_fields['pool'] = vm_config['pool']
        
        try:
            if existing_vm:
                # 更新現有虛擬機
                print(f"  更新現有虛擬機: {existing_vm.name}")
                
                update_data = {
                    'name': vm_name,
                    'cluster': cluster['id'],
                    'device': device.id,
                    'role': role_id,
                    'vcpus': self._calculate_vcpus(vm_config),
                    'memory': int(vm_config.get('memory', 0)),
                    'status': self._get_vm_status(vm_data, is_template),
                    'description': vm_config.get('description', ''),
                    'platform': platform_id,
                }
                
                if tag_ids:
                    update_data['tags'] = tag_ids
                if custom_fields:
                    update_data['custom_fields'] = custom_fields
                
                existing_vm.update(update_data)
                vm_obj = existing_vm
            else:
                # 創建新虛擬機
                vm_data_dict = {
                    'serial': vm_id,
                    'name': vm_name,
                    'cluster': cluster['id'],
                    'device': device.id,
                    'role': role_id,
                    'vcpus': self._calculate_vcpus(vm_config),
                    'memory': int(vm_config.get('memory', 0)),
                    'status': self._get_vm_status(vm_data, is_template),
                    'description': vm_config.get('description', ''),
                    'platform': platform_id,
                }
                
                if tag_ids:
                    vm_data_dict['tags'] = tag_ids
                if custom_fields:
                    vm_data_dict['custom_fields'] = custom_fields
                
                vm_obj = self.nb_api.virtualization.virtual_machines.create(**vm_data_dict)
                print(f"  創建虛擬機: {vm_name} (ID: {vm_obj.id})")
                
                # 更新緩存
                self.nb_objects['virtual_machines'][vm_obj.id] = vm_obj
                self.nb_objects['vm_by_serial'][vm_id] = vm_obj
                key = f"{vm_name}::{cluster['id']}"
                self.nb_objects['vm_by_name_cluster'][key] = vm_obj
            
            # 處理網絡接口
            interface_count = 0
            disk_size = 0
            disk_count = 0
            
            for config_key, config_value in vm_config.items():
                if config_key.startswith('net'):
                    interface_count += 1
                    self.process_vm_interface(vm_obj, config_key, config_value, agent_interfaces, mac_to_interface)
            
            # 處理虛擬磁盤
            for config_key, config_value in vm_config.items():
                if config_key.startswith(('scsi', 'virtio', 'sata', 'ide', 'efidisk', 'rootfs')):
                    if 'media=cdrom' not in str(config_value):
                        success, size_mb = self.process_virtual_disk(vm_obj, config_key, config_value)
                        if success:
                            disk_count += 1
                            disk_size += size_mb
            
            # 更新磁盤信息
            if disk_size > 0:
                try:
                    vm_obj.disk = disk_size
                    vm_obj.save()
                except Exception:
                    pass
            
            print(f"  標籤: {len(tag_ids)}個, 接口: {interface_count}個, 磁盤: {disk_count}個, 大小: {disk_size}MB")
            
            return True
            
        except Exception as e:
            print(f"  處理虛擬機失敗: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _calculate_vcpus(self, vm_config: Dict) -> int:
        """計算 CPU 核心數"""
        if 'vcpus' in vm_config:
            return int(vm_config['vcpus'])
        cores = int(vm_config.get('cores', 1))
        sockets = int(vm_config.get('sockets', 1))
        return cores * sockets
    
    def _get_vm_status(self, vm_data: Dict, is_template: bool) -> str:
        """獲取 VM 狀態"""
        if is_template:
            return 'staged'
        elif vm_data.get('status') == 'running':
            return 'active'
        else:
            return 'offline'
    
    def get_pve_nodes_info(self) -> List[Dict[str, Any]]:
        """獲取 PVE 節點信息"""
        try:
            return self.pve_api.nodes.get()
        except Exception as e:
            print(f"獲取 PVE 節點失敗: {e}")
            return []
    
    def get_pve_vms_info(self) -> Dict[str, List[Dict[str, Any]]]:
        """獲取 PVE 所有虛擬機信息 - 優化版本"""
        vms_by_node = {}
        
        try:
            nodes = self.pve_api.nodes.get()
            for node in nodes:
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
                        all_vms.append(vm)
                    for vm in lxc_vms:
                        vm['type'] = 'lxc'
                        all_vms.append(vm)
                    
                    vms_by_node[node_name] = all_vms
                except Exception as e:
                    print(f"  獲取節點 {node_name} 的虛擬機失敗: {e}")
                    vms_by_node[node_name] = []
        except Exception as e:
            print(f"獲取虛擬機信息失敗: {e}")
        
        return vms_by_node
    
    def get_or_create_site(self, site_name: str = "Main Datacenter") -> Optional[int]:
        """獲取或創建站點 - 使用緩存"""
        site_key = site_name.lower()
        
        if site_key in self.nb_objects['sites']:
            return self.nb_objects['sites'][site_key].id
        
        try:
            sites = list(self.nb_api.dcim.sites.filter(name=site_name))
            if sites:
                site = sites[0]
                self.nb_objects['sites'][site_key] = site
                return site.id
            
            site = self.nb_api.dcim.sites.create(
                name=site_name,
                slug=site_name.lower().replace(' ', '-'),
                status='active'
            )
            self.nb_objects['sites'][site_key] = site
            return site.id
            
        except Exception as e:
            print(f"處理站點失敗: {e}")
            return None
    
    def get_or_create_cluster_type(self, cluster_type_name: str = "Proxmox") -> Optional[int]:
        """獲取或創建集群類型 - 使用緩存"""
        cluster_type_key = cluster_type_name.lower()
        
        if cluster_type_key in self.nb_objects['cluster_types']:
            return self.nb_objects['cluster_types'][cluster_type_key].id
        
        try:
            cluster_types = list(self.nb_api.virtualization.cluster_types.filter(name=cluster_type_name))
            if cluster_types:
                cluster_type = cluster_types[0]
                self.nb_objects['cluster_types'][cluster_type_key] = cluster_type
                return cluster_type.id
            
            cluster_type = self.nb_api.virtualization.cluster_types.create(
                name=cluster_type_name,
                slug=cluster_type_name.lower().replace(' ', '-'),
                description="Proxmox Virtual Environment Cluster"
            )
            self.nb_objects['cluster_types'][cluster_type_key] = cluster_type
            return cluster_type.id
            
        except Exception as e:
            print(f"處理集群類型失敗: {e}")
            return None
    
    def get_or_create_cluster(self, cluster_name: str, site_id: int, cluster_type_id: int) -> Optional[Dict]:
        """獲取或創建集群 - 使用緩存"""
        cluster_key = cluster_name.lower()
        
        if cluster_key in self.nb_objects['clusters']:
            cluster = self.nb_objects['clusters'][cluster_key]
            return {'id': cluster.id, 'name': cluster.name}
        
        try:
            clusters = list(self.nb_api.virtualization.clusters.filter(name=cluster_name))
            if clusters:
                cluster = clusters[0]
                self.nb_objects['clusters'][cluster_key] = cluster
                return {'id': cluster.id, 'name': cluster.name}
            
            cluster = self.nb_api.virtualization.clusters.create(
                name=cluster_name,
                slug=cluster_name.lower().replace(' ', '-'),
                site=site_id,
                type=cluster_type_id
            )
            self.nb_objects['clusters'][cluster_key] = cluster
            return {'id': cluster.id, 'name': cluster.name}
            
        except Exception as e:
            print(f"處理集群失敗: {e}")
            return None
    
    def create_or_update_pve_node_device(self, node_info: Dict[str, Any], site_id: int, 
                                       device_type_id: int, device_role_id: int, cluster_id: int) -> Optional[Any]:
        """創建或更新 PVE 節點設備 - 優化版本"""
        node_name = node_info['node']
        node_key = node_name.lower()
        
        print(f"\n處理節點: {node_name}")
        
        # 檢查緩存
        if node_key in self.nb_objects['devices']:
            device = self.nb_objects['devices'][node_key]
            print(f"  設備已存在 (ID: {device.id})")
            
            # 更新設備狀態
            try:
                device.status = 'active' if node_info['status'] == 'online' else 'offline'
                device.save()
            except Exception as e:
                print(f"  更新設備失敗: {e}")
        else:
            # 創建新設備
            print("  創建新設備...")
            try:
                device = self.nb_api.dcim.devices.create(
                    name=node_name,
                    device_type=device_type_id,
                    role=device_role_id,
                    site=site_id,
                    status='active' if node_info['status'] == 'online' else 'offline',
                    serial=node_name,
                )
                self.nb_objects['devices'][node_key] = device
                print(f"  創建設備成功: {device.name} (ID: {device.id})")
            except Exception as e:
                print(f"  創建設備失敗: {e}")
                return None
        
        # 更新集群關聯
        try:
            device.cluster = cluster_id
            device.save()
        except Exception as e:
            print(f"  更新設備集群關聯失敗: {e}")
        
        return device
    
    def sync(self):
        """執行同步 - 優化版本"""
        print("開始 PVE 到 NetBox 同步")
        print("=" * 50)
        
        start_time = time.time()
        
        # 連接 API
        if not self.connect_pve():
            return
        if not self.connect_netbox():
            return
        
        # 加載 NetBox 對象到緩存
        self.load_netbox_objects()
        
        # 檢查 custom fields
        if not self.check_required_custom_fields():
            print("\n同步中止。")
            return
        
        # 顯示 PVE 信息
        self.show_pve_information()
        
        # 同步節點到 NetBox
        nodes_success, devices, cluster = self.sync_pve_nodes_to_netbox()
        
        if nodes_success and devices and cluster:
            print("\n" + "=" * 50)
            print("✓ 節點同步成功")
            print(f"集群: {cluster['name']} (ID: {cluster['id']})")
            print(f"設備數量: {len(devices)}")
            
            # 同步虛擬機
            print("\n" + "=" * 50)
            print("開始同步虛擬機...")
            
            vm_sync_start = time.time()
            if self.sync_pve_virtual_machines(devices, cluster):
                vm_sync_time = time.time() - vm_sync_start
                print(f"\n✓ 虛擬機同步成功 (耗時: {vm_sync_time:.2f}秒)")
            else:
                print("\n⚠ 虛擬機同步部分失敗")
        else:
            print("\n✗ 節點同步失敗")
        
        total_time = time.time() - start_time
        print(f"\n總同步時間: {total_time:.2f}秒")
    
    def sync_pve_nodes_to_netbox(self) -> Tuple[bool, Dict[str, Any], Dict]:
        """同步 PVE 節點到 NetBox - 優化版本"""
        print("\n開始同步 PVE 節點到 NetBox...")
        
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
        
        # 獲取設備類型和角色
        device_type_id = self._get_or_create_device_type()
        if not device_type_id:
            print("✗ 無法獲取設備類型")
            return False, {}, {}
        
        device_role_id = self._get_or_create_device_role()
        if not device_role_id:
            print("✗ 無法獲取設備角色")
            return False, {}, {}
        
        # 獲取 PVE 節點
        pve_nodes = self.get_pve_nodes_info()
        if not pve_nodes:
            print("✗ 沒有找到 PVE 節點")
            return False, {}, {}
        
        print(f"發現 {len(pve_nodes)} 個 PVE 節點:")
        for node in pve_nodes:
            print(f"  - {node['node']} (狀態: {node['status']})")
        
        # 創建或更新 NetBox 設備
        print("\n處理 NetBox 設備...")
        devices = {}
        success_count = 0
        
        for node in pve_nodes:
            device = self.create_or_update_pve_node_device(node, site_id, device_type_id, device_role_id, cluster['id'])
            if device:
                node_key = node['node'].lower()
                devices[node_key] = device
                success_count += 1
        
        print(f"\n節點同步結果: {success_count}/{len(pve_nodes)} 個節點已同步")
        
        if success_count > 0:
            return True, devices, cluster
        else:
            return False, {}, {}
    
    def _get_or_create_device_type(self) -> Optional[int]:
        """獲取或創建設備類型 - 使用緩存"""
        device_type_key = "proxmox node"
        
        if device_type_key in self.nb_objects['device_types']:
            return self.nb_objects['device_types'][device_type_key].id
        
        try:
            device_types = list(self.nb_api.dcim.device_types.filter(model="Proxmox Node"))
            if device_types:
                device_type = device_types[0]
                self.nb_objects['device_types'][device_type_key] = device_type
                return device_type.id
            
            # 獲取或創建製造商
            manufacturer_id = self._get_or_create_manufacturer()
            if not manufacturer_id:
                return None
            
            device_type = self.nb_api.dcim.device_types.create(
                manufacturer=manufacturer_id,
                model="Proxmox Node",
                slug="proxmox-node",
                is_full_depth=True
            )
            self.nb_objects['device_types'][device_type_key] = device_type
            return device_type.id
            
        except Exception as e:
            print(f"處理設備類型失敗: {e}")
            return None
    
    def _get_or_create_manufacturer(self) -> Optional[int]:
        """獲取或創建製造商 - 使用緩存"""
        manufacturer_key = "proxmox"
        
        if manufacturer_key in self.nb_objects['manufacturers']:
            return self.nb_objects['manufacturers'][manufacturer_key].id
        
        try:
            manufacturers = list(self.nb_api.dcim.manufacturers.filter(name="Proxmox"))
            if manufacturers:
                manufacturer = manufacturers[0]
                self.nb_objects['manufacturers'][manufacturer_key] = manufacturer
                return manufacturer.id
            
            manufacturer = self.nb_api.dcim.manufacturers.create(
                name="Proxmox",
                slug="proxmox"
            )
            self.nb_objects['manufacturers'][manufacturer_key] = manufacturer
            return manufacturer.id
            
        except Exception as e:
            print(f"處理製造商失敗: {e}")
            return None
    
    def _get_or_create_device_role(self) -> Optional[int]:
        """獲取或創建設備角色 - 使用緩存"""
        device_role_key = "hypervisor"
        
        if device_role_key in self.nb_objects['device_roles']:
            return self.nb_objects['device_roles'][device_role_key].id
        
        try:
            roles = list(self.nb_api.dcim.device_roles.filter(name="Hypervisor"))
            if roles:
                role = roles[0]
                self.nb_objects['device_roles'][device_role_key] = role
                return role.id
            
            role = self.nb_api.dcim.device_roles.create(
                name="Hypervisor",
                slug="hypervisor",
                color="2196f3"
            )
            self.nb_objects['device_roles'][device_role_key] = role
            return role.id
            
        except Exception as e:
            print(f"處理設備角色失敗: {e}")
            return None
    
    def sync_pve_virtual_machines(self, devices: Dict[str, Any], cluster: Dict) -> bool:
        """同步 PVE 虛擬機 - 優化版本"""
        print("\n開始同步虛擬機...")
        
        # 獲取 PVE 虛擬機信息
        vms_by_node = self.get_pve_vms_info()
        
        success_count = 0
        total_count = 0
        
        for node_name, vms in vms_by_node.items():
            print(f"\n處理節點 {node_name} 的虛擬機:")
            print(f"  發現 {len(vms)} 個虛擬機")
            
            # 獲取對應的 NetBox 設備
            device = devices.get(node_name.lower())
            if not device:
                print(f"  ✗ 找不到對應的設備: {node_name}")
                continue
            
            for vm in vms:
                total_count += 1
                if self.create_or_update_virtual_machine(node_name, vm, device, cluster):
                    success_count += 1
        
        print(f"\n虛擬機同步結果: {success_count}/{total_count} 個虛擬機已同步")
        return success_count > 0
    
    def show_pve_information(self):
        """顯示 PVE 信息"""
        print("\n" + "="*50)
        print("PVE 系統信息")
        print("="*50)
        
        # 獲取 PVE 版本
        try:
            version = self.pve_api.version.get()
            print(f"PVE 版本: {version['version']}")
        except Exception:
            pass
        
        # 獲取節點信息
        nodes = self.get_pve_nodes_info()
        print(f"\n發現 {len(nodes)} 個 PVE 節點:")
        for node in nodes:
            print(f"  - {node['node']} (狀態: {node['status']})")
        
        # 獲取虛擬機信息
        print("\n虛擬機統計:")
        vms_by_node = self.get_pve_vms_info()
        
        total_vms = 0
        qemu_count = 0
        lxc_count = 0
        
        for node_name, vms in vms_by_node.items():
            qemu_vms = [v for v in vms if v.get('type') == 'qemu']
            lxc_vms = [v for v in vms if v.get('type') == 'lxc']
            
            qemu_running = sum(1 for vm in qemu_vms if vm.get('status') == 'running')
            qemu_stopped = sum(1 for vm in qemu_vms if vm.get('status') == 'stopped')
            lxc_running = sum(1 for vm in lxc_vms if vm.get('status') == 'running')
            lxc_stopped = sum(1 for vm in lxc_vms if vm.get('status') == 'stopped')
            
            print(f"  {node_name}: {len(vms)} 個 VM (QEMU: {len(qemu_vms)}, LXC: {len(lxc_vms)})")
            print(f"    QEMU: 運行 {qemu_running}, 停止 {qemu_stopped}")
            print(f"    LXC: 運行 {lxc_running}, 停止 {lxc_stopped}")
            
            total_vms += len(vms)
            qemu_count += len(qemu_vms)
            lxc_count += len(lxc_vms)
        
        print(f"\n總計: {len(nodes)} 個節點, {total_vms} 個虛擬機 (QEMU: {qemu_count}, LXC: {lxc_count})")
        print("="*50)


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
    ]
    
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        print(f"✗ 缺少必要環境變量: {', '.join(missing_vars)}")
        print("請設置以下環境變量:")
        for var in required_env_vars:
            print(f"  export {var}=value")
        sys.exit(1)
    
    # 創建同步器並執行
    sync = PVEToNetBoxSync()
    sync.sync()


if __name__ == '__main__':
    main()