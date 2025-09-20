#!/usr/bin/env python3
"""
Industrial Sensor Data Streaming Generator

This script generates real-time streaming data for industrial sensor monitoring
and anomaly detection projects. It simulates continuous data flow from multiple
machines with realistic failure patterns and supports multiple output formats.

Features:
- Real-time data streaming with configurable rates
- Multiple output modes (console, file, API, Kafka)
- Real-time anomaly injection
- Maintenance event simulation
- Interactive control for testing
- Consistent with existing dataset structure

Usage:
    python generate_flow.py --mode console --rate 1.0
    python generate_flow.py --mode file --output data/stream.csv --rate 5.0
    python generate_flow.py --mode api --url http://localhost:8000/api/data --rate 2.0
    python generate_flow.py --mode kafka --topic sensor-data --rate 10.0

Output formats:
    - Console: Real-time display for testing
    - File: Continuous CSV/JSON file writing
    - API: HTTP POST requests to endpoints
    - Kafka: Message queue streaming
"""

import pandas as pd
import numpy as np
import json
import time
import argparse
import threading
import queue
import requests
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
import warnings
from dataclasses import dataclass, asdict
from enum import Enum

# Optional imports for advanced features
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

warnings.filterwarnings('ignore')

class OutputMode(Enum):
    """Available output modes for streaming data"""
    CONSOLE = "console"
    FILE = "file"
    API = "api"
    KAFKA = "kafka"

@dataclass
class StreamingConfig:
    """Configuration for streaming data generation"""
    # Basic settings
    rate: float = 1.0  # Hz - data points per second
    duration: Optional[int] = None  # seconds, None for infinite
    machines: List[str] = None  # None for all machines
    
    # Output settings
    output_mode: OutputMode = OutputMode.CONSOLE
    output_path: Optional[str] = None
    api_url: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_bootstrap_servers: List[str] = None
    
    # Data settings
    include_anomalies: bool = True
    anomaly_injection_rate: float = 0.01  # probability per data point
    maintenance_simulation: bool = True
    maintenance_interval: int = 3600  # seconds between maintenance
    
    # Format settings
    data_format: str = "json"  # json, csv
    include_derived_features: bool = True
    include_anomaly_labels: bool = False  # Default to False for student projects
    
    # Interactive settings
    interactive_mode: bool = False
    verbose: bool = True

class IndustrialSystemConfig:
    """Configuration class for the industrial system simulation - adapted for streaming"""
    
    def __init__(self):
        # Time configuration
        self.start_date = datetime(2024, 1, 1)
        self.current_time = datetime.now()
        
        # Machine configuration
        self.num_machines = 5
        self.machine_ids = [f'MACHINE_{i:03d}' for i in range(1, self.num_machines + 1)]
        
        # Sensor configuration with realistic industrial values
        self.sensors = {
            'temperature': {
                'unit': '°C', 
                'normal_range': (60, 85), 
                'critical_threshold': 95,
                'description': 'Main bearing temperature'
            },
            'vibration': {
                'unit': 'mm/s', 
                'normal_range': (2, 8), 
                'critical_threshold': 12,
                'description': 'Vibration amplitude'
            },
            'pressure': {
                'unit': 'bar', 
                'normal_range': (4.5, 6.5), 
                'critical_threshold': 8.0,
                'description': 'Hydraulic system pressure'
            },
            'flow_rate': {
                'unit': 'L/min', 
                'normal_range': (180, 220), 
                'critical_threshold': 250,
                'description': 'Coolant flow rate'
            },
            'power_consumption': {
                'unit': 'kW', 
                'normal_range': (45, 55), 
                'critical_threshold': 65,
                'description': 'Motor power consumption'
            },
            'oil_level': {
                'unit': '%', 
                'normal_range': (75, 95), 
                'critical_threshold': 60,
                'description': 'Lubrication oil level'
            },
            'bearing_temperature': {
                'unit': '°C', 
                'normal_range': (70, 90), 
                'critical_threshold': 105,
                'description': 'Secondary bearing temperature'
            }
        }
        
        # Failure patterns with realistic probabilities and affected sensors
        self.failure_types = {
            'bearing_failure': {
                'probability': 0.3, 
                'sensors_affected': ['vibration', 'bearing_temperature'],
                'description': 'Gradual bearing wear leading to increased vibration and temperature'
            },
            'pump_failure': {
                'probability': 0.25, 
                'sensors_affected': ['pressure', 'flow_rate', 'power_consumption'],
                'description': 'Pump degradation causing pressure drops and flow rate issues'
            },
            'overheating': {
                'probability': 0.2, 
                'sensors_affected': ['temperature', 'power_consumption'],
                'description': 'System overheating due to cooling system failure'
            },
            'oil_leak': {
                'probability': 0.15, 
                'sensors_affected': ['oil_level', 'temperature'],
                'description': 'Oil leak causing lubrication issues and temperature rise'
            },
            'motor_failure': {
                'probability': 0.1, 
                'sensors_affected': ['power_consumption', 'vibration', 'temperature'],
                'description': 'Motor electrical issues causing erratic behavior'
            }
        }

class StreamingDataGenerator:
    """Main class for generating streaming industrial sensor data"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.system_config = IndustrialSystemConfig()
        self.running = False
        self.data_queue = queue.Queue()
        self.anomaly_events = {}  # Track ongoing anomalies per machine
        self.maintenance_events = {}  # Track maintenance periods
        self.machine_states = {}  # Track machine operational states
        
        # Initialize machine states
        for machine_id in self.system_config.machine_ids:
            self.machine_states[machine_id] = {
                'operational': True,
                'current_anomaly': None,
                'anomaly_start_time': None,
                'last_maintenance': self.system_config.current_time
            }
        
        # Setup output handlers
        self._setup_output_handlers()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_output_handlers(self):
        """Setup output handlers based on configuration"""
        if self.config.output_mode == OutputMode.FILE:
            self._setup_file_output()
        elif self.config.output_mode == OutputMode.API:
            self._setup_api_output()
        elif self.config.output_mode == OutputMode.KAFKA:
            self._setup_kafka_output()
    
    def _setup_file_output(self):
        """Setup file output handler"""
        if not self.config.output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.config.output_path = f"streaming_data_{timestamp}.{self.config.data_format}"
        
        self.output_file = Path(self.config.output_path)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write header for CSV format
        if self.config.data_format == "csv":
            with open(self.output_file, 'w') as f:
                f.write("timestamp,machine_id,maintenance_status,")
                f.write(",".join(self.system_config.sensors.keys()))
                if self.config.include_anomaly_labels:
                    f.write(",anomaly_label,anomaly_type")
                f.write("\n")
    
    def _setup_api_output(self):
        """Setup API output handler"""
        if not self.config.api_url:
            raise ValueError("API URL must be specified for API output mode")
        
        # Test API connectivity
        try:
            response = requests.get(self.config.api_url, timeout=5)
            if self.config.verbose:
                print(f"✅ API connectivity verified: {self.config.api_url}")
        except requests.exceptions.RequestException as e:
            if self.config.verbose:
                print(f"⚠️  API connectivity warning: {e}")
    
    def _setup_kafka_output(self):
        """Setup Kafka output handler"""
        if not KAFKA_AVAILABLE:
            raise ImportError("Kafka support not available. Install with: pip install kafka-python")
        
        if not self.config.kafka_topic:
            raise ValueError("Kafka topic must be specified for Kafka output mode")
        
        bootstrap_servers = self.config.kafka_bootstrap_servers or ['localhost:9092']
        
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )
            if self.config.verbose:
                print(f"✅ Kafka producer connected: {bootstrap_servers}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Kafka: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        if self.config.verbose:
            print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def generate_normal_operation(self, machine_id: str, sensor_name: str, timestamp: datetime) -> float:
        """Generate normal operation data for a sensor with realistic variations"""
        sensor_config = self.system_config.sensors[sensor_name]
        normal_min, normal_max = sensor_config['normal_range']
        
        # Base value with some natural variation
        base_value = (normal_min + normal_max) / 2
        
        # Add realistic variations
        # 1. Daily cycle (equipment runs differently during day/night)
        daily_cycle = 2 * np.sin(2 * np.pi * timestamp.hour / 24)
        
        # 2. Weekly cycle (weekend vs weekday patterns)
        weekly_cycle = 1.5 * np.sin(2 * np.pi * timestamp.weekday() / 7)
        
        # 3. Random noise
        noise = np.random.normal(0, (normal_max - normal_min) * 0.05)
        
        # 4. Gradual drift (equipment aging)
        drift = (timestamp - self.system_config.start_date).days * 0.001
        
        # 5. Machine-specific variations
        machine_factor = hash(machine_id) % 100 / 1000  # Small machine-specific offset
        
        value = base_value + daily_cycle + weekly_cycle + noise + drift + machine_factor
        
        # Ensure value stays within normal range
        return np.clip(value, normal_min, normal_max)
    
    def generate_anomaly_pattern(self, machine_id: str, sensor_name: str, failure_type: str, 
                               timestamp: datetime, anomaly_start: datetime) -> float:
        """Generate realistic anomaly pattern for a specific failure type"""
        sensor_config = self.system_config.sensors[sensor_name]
        normal_min, normal_max = sensor_config['normal_range']
        critical_threshold = sensor_config['critical_threshold']
        
        # Time since anomaly started
        time_since_anomaly = (timestamp - anomaly_start).total_seconds() / 3600  # hours
        
        if failure_type == 'bearing_failure':
            if sensor_name == 'vibration':
                # Gradual increase in vibration
                increase_rate = 0.5  # mm/s per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            elif sensor_name == 'bearing_temperature':
                # Temperature rises with vibration
                increase_rate = 2.0  # °C per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            else:
                return self.generate_normal_operation(machine_id, sensor_name, timestamp)
                
        elif failure_type == 'pump_failure':
            if sensor_name == 'pressure':
                # Pressure drops as pump fails
                decrease_rate = 0.1  # bar per hour
                anomaly_value = normal_max - (decrease_rate * time_since_anomaly)
            elif sensor_name == 'flow_rate':
                # Flow rate decreases
                decrease_rate = 2.0  # L/min per hour
                anomaly_value = normal_max - (decrease_rate * time_since_anomaly)
            elif sensor_name == 'power_consumption':
                # Power consumption increases as pump struggles
                increase_rate = 0.8  # kW per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            else:
                return self.generate_normal_operation(machine_id, sensor_name, timestamp)
                
        elif failure_type == 'overheating':
            if sensor_name == 'temperature':
                # Rapid temperature increase
                increase_rate = 3.0  # °C per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            elif sensor_name == 'power_consumption':
                # Power consumption increases with temperature
                increase_rate = 1.2  # kW per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            else:
                return self.generate_normal_operation(machine_id, sensor_name, timestamp)
                
        elif failure_type == 'oil_leak':
            if sensor_name == 'oil_level':
                # Gradual oil level decrease
                decrease_rate = 0.5  # % per hour
                anomaly_value = normal_max - (decrease_rate * time_since_anomaly)
            elif sensor_name == 'temperature':
                # Temperature increases due to reduced lubrication
                increase_rate = 1.5  # °C per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            else:
                return self.generate_normal_operation(machine_id, sensor_name, timestamp)
                
        elif failure_type == 'motor_failure':
            if sensor_name == 'power_consumption':
                # Power consumption becomes erratic
                base_increase = 2.0  # kW
                erratic_component = 3.0 * np.sin(time_since_anomaly * 2)  # Oscillating pattern
                anomaly_value = normal_max + base_increase + erratic_component
            elif sensor_name == 'vibration':
                # Vibration increases and becomes irregular
                base_increase = 3.0  # mm/s
                erratic_component = 2.0 * np.sin(time_since_anomaly * 3)
                anomaly_value = normal_max + base_increase + erratic_component
            elif sensor_name == 'temperature':
                # Temperature increases
                increase_rate = 2.5  # °C per hour
                anomaly_value = normal_max + (increase_rate * time_since_anomaly)
            else:
                return self.generate_normal_operation(machine_id, sensor_name, timestamp)
        
        # Add some noise to make it more realistic
        noise = np.random.normal(0, (normal_max - normal_min) * 0.02)
        anomaly_value += noise
        
        # Ensure value doesn't exceed critical threshold immediately
        return np.clip(anomaly_value, normal_min, critical_threshold)
    
    def generate_data_point(self, machine_id: str, timestamp: datetime) -> Dict:
        """Generate a single data point for a machine"""
        machine_state = self.machine_states[machine_id]
        
        # Check if machine is in maintenance
        if machine_state['operational'] == False:
            maintenance_status = 'maintenance'
        else:
            maintenance_status = 'operational'
        
        # Generate sensor data
        sensor_data = {}
        for sensor_name in self.system_config.sensors.keys():
            if (machine_state['current_anomaly'] and 
                sensor_name in self.system_config.failure_types[machine_state['current_anomaly']]['sensors_affected']):
                value = self.generate_anomaly_pattern(
                    machine_id, sensor_name, machine_state['current_anomaly'], 
                    timestamp, machine_state['anomaly_start_time']
                )
            else:
                value = self.generate_normal_operation(machine_id, sensor_name, timestamp)
            
            sensor_data[sensor_name] = round(value, 2)
        
        # Create data point
        data_point = {
            'timestamp': timestamp.isoformat(),
            'machine_id': machine_id,
            'maintenance_status': maintenance_status,
            **sensor_data
        }
        
        # Add anomaly labels if requested (for instructor/testing use)
        if self.config.include_anomaly_labels:
            if machine_state['current_anomaly']:
                data_point['anomaly_label'] = 1
                data_point['anomaly_type'] = machine_state['current_anomaly']
            else:
                data_point['anomaly_label'] = 0
                data_point['anomaly_type'] = 'normal'
        
        # Add derived features if requested
        if self.config.include_derived_features:
            data_point.update(self._calculate_derived_features(timestamp, sensor_data))
        
        return data_point
    
    def _calculate_derived_features(self, timestamp: datetime, sensor_data: Dict) -> Dict:
        """Calculate derived features for a data point"""
        features = {
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'month': timestamp.month,
            'is_weekend': 1 if timestamp.weekday() >= 5 else 0,
            'is_night_shift': 1 if timestamp.hour >= 22 or timestamp.hour <= 6 else 0
        }
        
        # Add cross-sensor features
        if 'temperature' in sensor_data and 'pressure' in sensor_data:
            features['temp_pressure_ratio'] = round(sensor_data['temperature'] / sensor_data['pressure'], 4)
        
        if 'vibration' in sensor_data and 'power_consumption' in sensor_data:
            features['vibration_power_ratio'] = round(sensor_data['vibration'] / sensor_data['power_consumption'], 4)
        
        if 'flow_rate' in sensor_data and 'pressure' in sensor_data:
            features['flow_pressure_product'] = round(sensor_data['flow_rate'] * sensor_data['pressure'], 2)
        
        return features
    
    def inject_anomaly(self, machine_id: str, failure_type: str = None):
        """Manually inject an anomaly for testing purposes"""
        if machine_id not in self.machine_states:
            raise ValueError(f"Unknown machine: {machine_id}")
        
        if failure_type is None:
            failure_type = np.random.choice(
                list(self.system_config.failure_types.keys()),
                p=[self.system_config.failure_types[ft]['probability'] for ft in self.system_config.failure_types.keys()]
            )
        
        self.machine_states[machine_id]['current_anomaly'] = failure_type
        self.machine_states[machine_id]['anomaly_start_time'] = datetime.now()
        
        if self.config.verbose:
            print(f"🚨 Injected {failure_type} anomaly for {machine_id}")
    
    def clear_anomaly(self, machine_id: str):
        """Clear anomaly for a machine"""
        if machine_id in self.machine_states:
            self.machine_states[machine_id]['current_anomaly'] = None
            self.machine_states[machine_id]['anomaly_start_time'] = None
            if self.config.verbose:
                print(f"✅ Cleared anomaly for {machine_id}")
    
    def simulate_maintenance(self, machine_id: str, duration_minutes: int = 30):
        """Simulate maintenance period for a machine"""
        if machine_id in self.machine_states:
            self.machine_states[machine_id]['operational'] = False
            self.machine_states[machine_id]['last_maintenance'] = datetime.now()
            
            # Schedule end of maintenance
            def end_maintenance():
                time.sleep(duration_minutes * 60)
                self.machine_states[machine_id]['operational'] = True
                if self.config.verbose:
                    print(f"🔧 Maintenance completed for {machine_id}")
            
            threading.Thread(target=end_maintenance, daemon=True).start()
            
            if self.config.verbose:
                print(f"🔧 Started maintenance for {machine_id} (duration: {duration_minutes} minutes)")
    
    def _output_data_point(self, data_point: Dict):
        """Output a single data point based on configured mode"""
        if self.config.output_mode == OutputMode.CONSOLE:
            self._output_to_console(data_point)
        elif self.config.output_mode == OutputMode.FILE:
            self._output_to_file(data_point)
        elif self.config.output_mode == OutputMode.API:
            self._output_to_api(data_point)
        elif self.config.output_mode == OutputMode.KAFKA:
            self._output_to_kafka(data_point)
    
    def _output_to_console(self, data_point: Dict):
        """Output data point to console"""
        if self.config.data_format == "json":
            print(json.dumps(data_point, indent=2))
        else:  # csv-like format
            values = [str(data_point.get(key, '')) for key in ['timestamp', 'machine_id', 'maintenance_status']]
            values.extend([str(data_point.get(sensor, '')) for sensor in self.system_config.sensors.keys()])
            if self.config.include_anomaly_labels:
                values.extend([str(data_point.get('anomaly_label', '')), str(data_point.get('anomaly_type', ''))])
            print(",".join(values))
    
    def _output_to_file(self, data_point: Dict):
        """Output data point to file"""
        if self.config.data_format == "json":
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(data_point) + '\n')
        else:  # csv format
            with open(self.output_file, 'a') as f:
                values = [str(data_point.get(key, '')) for key in ['timestamp', 'machine_id', 'maintenance_status']]
                values.extend([str(data_point.get(sensor, '')) for sensor in self.system_config.sensors.keys()])
                if self.config.include_anomaly_labels:
                    values.extend([str(data_point.get('anomaly_label', '')), str(data_point.get('anomaly_type', ''))])
                f.write(",".join(values) + '\n')
    
    def _output_to_api(self, data_point: Dict):
        """Output data point to API endpoint"""
        try:
            response = requests.post(
                self.config.api_url,
                json=data_point,
                timeout=5
            )
            if response.status_code != 200 and self.config.verbose:
                print(f"⚠️  API request failed: {response.status_code}")
        except requests.exceptions.RequestException as e:
            if self.config.verbose:
                print(f"⚠️  API request error: {e}")
    
    def _output_to_kafka(self, data_point: Dict):
        """Output data point to Kafka topic"""
        try:
            self.kafka_producer.send(
                self.config.kafka_topic,
                key=data_point['machine_id'],
                value=data_point
            )
        except Exception as e:
            if self.config.verbose:
                print(f"⚠️  Kafka send error: {e}")
    
    def _anomaly_injection_thread(self):
        """Background thread for automatic anomaly injection"""
        while self.running:
            try:
                time.sleep(1.0 / self.config.anomaly_injection_rate)  # Adjust injection rate
                
                if np.random.random() < self.config.anomaly_injection_rate:
                    # Select random machine
                    machine_id = np.random.choice(self.system_config.machine_ids)
                    
                    # Only inject if machine is operational and not already in anomaly
                    if (self.machine_states[machine_id]['operational'] and 
                        not self.machine_states[machine_id]['current_anomaly']):
                        
                        failure_type = np.random.choice(
                            list(self.system_config.failure_types.keys()),
                            p=[self.system_config.failure_types[ft]['probability'] for ft in self.system_config.failure_types.keys()]
                        )
                        
                        self.inject_anomaly(machine_id, failure_type)
                        
                        # Schedule anomaly end (random duration)
                        duration_hours = np.random.uniform(2, 24)  # 2-24 hours
                        threading.Timer(
                            duration_hours * 3600,
                            self.clear_anomaly,
                            args=[machine_id]
                        ).start()
                        
            except Exception as e:
                if self.config.verbose:
                    print(f"⚠️  Anomaly injection error: {e}")
    
    def _maintenance_simulation_thread(self):
        """Background thread for maintenance simulation"""
        while self.running:
            try:
                time.sleep(self.config.maintenance_interval)
                
                # Random maintenance for a machine
                machine_id = np.random.choice(self.system_config.machine_ids)
                duration_minutes = np.random.randint(15, 120)  # 15-120 minutes
                
                self.simulate_maintenance(machine_id, duration_minutes)
                
            except Exception as e:
                if self.config.verbose:
                    print(f"⚠️  Maintenance simulation error: {e}")
    
    def _interactive_control_thread(self):
        """Interactive control thread for manual testing"""
        print("\n🎮 Interactive Controls:")
        print("  'a <machine_id> [failure_type]' - Inject anomaly")
        print("  'c <machine_id>' - Clear anomaly")
        print("  'm <machine_id> [duration]' - Start maintenance")
        print("  's' - Show machine states")
        print("  'q' - Quit")
        print("  'h' - Show this help")
        
        while self.running:
            try:
                command = input("\n> ").strip().split()
                if not command:
                    continue
                
                cmd = command[0].lower()
                
                if cmd == 'q':
                    self.running = False
                    break
                elif cmd == 'h':
                    print("🎮 Interactive Controls:")
                    print("  'a <machine_id> [failure_type]' - Inject anomaly")
                    print("  'c <machine_id>' - Clear anomaly")
                    print("  'm <machine_id> [duration]' - Start maintenance")
                    print("  's' - Show machine states")
                    print("  'q' - Quit")
                elif cmd == 's':
                    print("\n📊 Machine States:")
                    for machine_id, state in self.machine_states.items():
                        status = "🔧 Maintenance" if not state['operational'] else "✅ Operational"
                        anomaly = f"🚨 {state['current_anomaly']}" if state['current_anomaly'] else "✅ Normal"
                        print(f"  {machine_id}: {status} | {anomaly}")
                elif cmd == 'a' and len(command) >= 2:
                    machine_id = command[1]
                    failure_type = command[2] if len(command) > 2 else None
                    self.inject_anomaly(machine_id, failure_type)
                elif cmd == 'c' and len(command) >= 2:
                    machine_id = command[1]
                    self.clear_anomaly(machine_id)
                elif cmd == 'm' and len(command) >= 2:
                    machine_id = command[1]
                    duration = int(command[2]) if len(command) > 2 else 30
                    self.simulate_maintenance(machine_id, duration)
                else:
                    print("❌ Unknown command. Type 'h' for help.")
                    
            except (EOFError, KeyboardInterrupt):
                break
            except Exception as e:
                print(f"❌ Command error: {e}")
    
    def start_streaming(self):
        """Start the streaming data generation"""
        if self.config.verbose:
            print("🚀 Starting Industrial Sensor Data Streaming")
            print(f"📊 Configuration:")
            print(f"  Rate: {self.config.rate} Hz")
            print(f"  Duration: {'Infinite' if self.config.duration is None else f'{self.config.duration}s'}")
            print(f"  Machines: {self.config.machines or 'All'}")
            print(f"  Output: {self.config.output_mode.value}")
            print(f"  Format: {self.config.data_format}")
            print(f"  Anomalies: {'Enabled' if self.config.include_anomalies else 'Disabled'}")
            print(f"  Interactive: {'Enabled' if self.config.interactive_mode else 'Disabled'}")
            print()
        
        self.running = True
        start_time = time.time()
        
        # Start background threads
        threads = []
        
        if self.config.include_anomalies and self.config.anomaly_injection_rate > 0:
            anomaly_thread = threading.Thread(target=self._anomaly_injection_thread, daemon=True)
            anomaly_thread.start()
            threads.append(anomaly_thread)
        
        if self.config.maintenance_simulation:
            maintenance_thread = threading.Thread(target=self._maintenance_simulation_thread, daemon=True)
            maintenance_thread.start()
            threads.append(maintenance_thread)
        
        if self.config.interactive_mode:
            interactive_thread = threading.Thread(target=self._interactive_control_thread, daemon=True)
            interactive_thread.start()
            threads.append(interactive_thread)
        
        # Main streaming loop
        try:
            while self.running:
                loop_start = time.time()
                
                # Generate data for each machine
                machines_to_process = self.config.machines or self.system_config.machine_ids
                
                for machine_id in machines_to_process:
                    if not self.running:
                        break
                    
                    timestamp = datetime.now()
                    data_point = self.generate_data_point(machine_id, timestamp)
                    self._output_data_point(data_point)
                
                # Check duration limit
                if self.config.duration and (time.time() - start_time) >= self.config.duration:
                    if self.config.verbose:
                        print(f"\n⏰ Duration limit reached ({self.config.duration}s)")
                    break
                
                # Maintain streaming rate
                elapsed = time.time() - loop_start
                sleep_time = max(0, (1.0 / self.config.rate) - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            if self.config.verbose:
                print("\n🛑 Streaming interrupted by user")
        finally:
            self.running = False
            
            # Cleanup
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            
            if self.config.verbose:
                print("✅ Streaming stopped gracefully")

def load_config_from_file(config_path: str) -> StreamingConfig:
    """Load configuration from YAML file"""
    if not YAML_AVAILABLE:
        raise ImportError("YAML support not available. Install with: pip install pyyaml")
    
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Convert to StreamingConfig
    config = StreamingConfig()
    for key, value in config_data.get('streaming', {}).items():
        if hasattr(config, key):
            if key == 'output_mode' and isinstance(value, str):
                value = OutputMode(value)
            setattr(config, key, value)
    
    return config

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description="Industrial Sensor Data Streaming Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Student projects - clean data (no anomaly labels)
  python generate_flow.py --mode console --rate 1.0
  python generate_flow.py --mode file --output data/stream.csv --rate 5.0
  
  # Instructor testing - data with labels for grading
  python generate_flow.py --mode file --output instructor_data.csv --rate 1.0 --include-labels
  
  # API and Kafka streaming
  python generate_flow.py --mode api --url http://localhost:8000/api/data --rate 2.0
  python generate_flow.py --mode kafka --topic sensor-data --rate 10.0
  
  # Interactive mode for testing
  python generate_flow.py --mode console --interactive --rate 1.0
  
  # Load configuration from file
  python generate_flow.py --config config/streaming.yaml
        """
    )
    
    # Basic settings
    parser.add_argument('--rate', type=float, default=1.0, help='Streaming rate in Hz (default: 1.0)')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: infinite)')
    parser.add_argument('--machines', nargs='+', help='Machine IDs to stream (default: all)')
    
    # Output settings
    parser.add_argument('--mode', choices=['console', 'file', 'api', 'kafka'], 
                       default='console', help='Output mode (default: console)')
    parser.add_argument('--output', help='Output file path (for file mode)')
    parser.add_argument('--url', help='API URL (for api mode)')
    parser.add_argument('--topic', help='Kafka topic (for kafka mode)')
    parser.add_argument('--bootstrap-servers', nargs='+', default=['localhost:9092'],
                       help='Kafka bootstrap servers (default: localhost:9092)')
    
    # Data settings
    parser.add_argument('--no-anomalies', action='store_true', help='Disable anomaly injection')
    parser.add_argument('--anomaly-rate', type=float, default=0.01, help='Anomaly injection rate (default: 0.01)')
    parser.add_argument('--no-maintenance', action='store_true', help='Disable maintenance simulation')
    parser.add_argument('--maintenance-interval', type=int, default=3600, help='Maintenance interval in seconds (default: 3600)')
    
    # Format settings
    parser.add_argument('--format', choices=['json', 'csv'], default='json', help='Data format (default: json)')
    parser.add_argument('--no-derived-features', action='store_true', help='Disable derived features')
    parser.add_argument('--no-labels', action='store_true', help='Disable anomaly labels (default: disabled for student projects)')
    parser.add_argument('--include-labels', action='store_true', help='Include anomaly labels (for instructor testing/grading)')
    
    # Interactive settings
    parser.add_argument('--interactive', action='store_true', help='Enable interactive mode')
    parser.add_argument('--quiet', action='store_true', help='Disable verbose output')
    
    # Configuration file
    parser.add_argument('--config', help='Load configuration from YAML file')
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        config = load_config_from_file(args.config)
    else:
        # Create configuration from command line arguments
        config = StreamingConfig(
            rate=args.rate,
            duration=args.duration,
            machines=args.machines,
            output_mode=OutputMode(args.mode),
            output_path=args.output,
            api_url=args.url,
            kafka_topic=args.topic,
            kafka_bootstrap_servers=args.bootstrap_servers,
            include_anomalies=not args.no_anomalies,
            anomaly_injection_rate=args.anomaly_rate,
            maintenance_simulation=not args.no_maintenance,
            maintenance_interval=args.maintenance_interval,
            data_format=args.format,
            include_derived_features=not args.no_derived_features,
            include_anomaly_labels=args.include_labels,
            interactive_mode=args.interactive,
            verbose=not args.quiet
        )
    
    # Validate configuration
    if config.output_mode == OutputMode.FILE and not config.output_path:
        config.output_path = f"streaming_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{config.data_format}"
    
    if config.output_mode == OutputMode.API and not config.api_url:
        parser.error("API URL is required for API output mode")
    
    if config.output_mode == OutputMode.KAFKA and not config.kafka_topic:
        parser.error("Kafka topic is required for Kafka output mode")
    
    # Create and start generator
    try:
        generator = StreamingDataGenerator(config)
        generator.start_streaming()
    except KeyboardInterrupt:
        print("\n🛑 Streaming interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
