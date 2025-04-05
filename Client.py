# distributed_plate_recognition.py (Main Program)
import argparse
import pika
import json
import os
import time
import pandas as pd
from datetime import datetime

class ResultCollector:
    def __init__(self, total_tasks):
        self.total = total_tasks
        self.results = []
        self.start_time = time.time()
        
    def callback(self, ch, method, properties, body):
        result = json.loads(body)
        self.results.append((result['filename'], result['plate_number']))
        
        if len(self.results) >= self.total:
            # Generate output files
            df = pd.DataFrame(self.results, columns=['filename', 'plate_number'])
            df = df[df['plate_number'].notna() & (df['plate_number'] != '')]
            
            # Create output directory
            os.makedirs('results', exist_ok=True)
            
            # Save CSV
            csv_path = f'results/results_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
            df.to_csv(csv_path, index=False)
            
            # Save stats
            stats = f"""Number of images processed: {self.total} Total plates identified: {len(df)} Processing time: {time.time() - self.start_time:.2f} seconds"""
            
            with open(f'results/stats_{datetime.now().strftime("%Y%m%d%H%M%S")}.txt', 'w') as f:
                f.write(stats)
            
            print("Processing completed!")
            print(f"Results saved to {csv_path}")
            exit(0)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('image_folder', help='Path to image folder')
    parser.add_argument('nodes', type=int, help='Number of worker nodes')
    args = parser.parse_args()

    # Get image files
    image_files = [os.path.join(args.image_folder, f) for f in os.listdir(args.image_folder) 
                   if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))]
    
    # RabbitMQ connection
    credentials = pika.PlainCredentials('rabbituser','rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.2.12.62',5672,'/',credentials))
    channel = connection.channel()
    
    # Declare queues
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue')
    
    # Send tasks
    for img_path in image_files:
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps({'image_path': img_path}),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    print(f"Sent {len(image_files)} tasks to queue")
    
    # Start result collector
    collector = ResultCollector(len(image_files))
    channel.basic_consume(queue='result_queue', 
                         on_message_callback=collector.callback,
                         auto_ack=True)
    
    print("Waiting for results...")
    channel.start_consuming()

if __name__ == '__main__':
    main()
