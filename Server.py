# worker.py (Worker Node)
import pika
import json
import cv2
import pytesseract
import numpy as np
import os

def detect_license_plate(image_path):
    try:
        # 1. Image Preprocessing
        img = cv2.imread(image_path)
        if img is None:
            print(f"Error: Could not read image at {image_path}")
            return os.path.basename(image_path), None

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        gray = cv2.bilateralFilter(gray, 11, 17, 17) # Reduce noise while preserving edges

        # 2. License Plate Detection
        edged = cv2.Canny(gray, 30, 200)
        contours, _ = cv2.findContours(edged.copy(), cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        contours = sorted(contours, key=cv2.contourArea, reverse=True)[:10]

        location = None
        for contour in contours:
            approx = cv2.approxPolyDP(contour, 10, True)
            if len(approx) == 4:  # if rectangle
                location = approx
                break
        
        if location is not None:
            mask = np.zeros(gray.shape, np.uint8)
            cv2.drawContours(mask, [location], 0, 255, -1)
            new_image = cv2.bitwise_and(img, img, mask=mask)

            # bound box
            x, y, w, h = cv2.boundingRect(location)
            cropped_image = gray[y:y + h, x:x + w]
        else:
            # Fallback
            edged = cv2.Canny(gray, 30, 200)
            contours, _ = cv2.findContours(edged.copy(), cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
            
            # Refine contour selection using multiple criteria:
            plate_contours = []
            for c in contours:
                x, y, w, h = cv2.boundingRect(c) # Get bounding rectangle
                aspect_ratio = float(w) / h
                area = w * h

                if 2 <= aspect_ratio <= 7 and area > 1000 and 100 < w < 1000 and 50 < h < 500: # Adjust thresholds
                    plate_contours.append(c)

            if not plate_contours:
                return os.path.basename(image_path), None

            # Select the largest contour (most likely the plate):
            plate_contour = max(plate_contours, key=cv2.contourArea)
            x, y, w, h = cv2.boundingRect(plate_contour)
            cropped_image = gray[y:y + h, x:x + w]

        # Enhance image for OCR
        cropped_image = cv2.equalizeHist(cropped_image)
        
        # 4. OCR using pytesseract (with improved configuration)
        config = ('-l eng --oem 1 --psm 8') # Adjust as needed.  PSM 8 treats as a single word.
        plate_number = pytesseract.image_to_string(cropped_image, config=config)
        plate_number = ''.join(filter(str.isalnum, plate_number)).strip()
        
        print(f"Processed image: {image_path}")
        
        return os.path.basename(image_path), plate_number

    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return os.path.basename(image_path), None

def main():
    credentials = pika.PlainCredentials('rabbituser','rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.2.12.62',5672,'/',credentials))
    channel = connection.channel()
    
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue')
    
    def callback(ch, method, properties, body):
        task = json.loads(body)
        filename, plate = detect_license_plate(task['image_path'])
        
        # Send result
        channel.basic_publish(
            exchange='',
            routing_key='result_queue',
            body=json.dumps({'filename': filename, 'plate_number': plate})
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    print("Worker ready. Waiting for tasks...")
    channel.start_consuming()

if __name__ == '__main__':
    main()
