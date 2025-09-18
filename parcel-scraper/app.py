from flask import Flask, request, send_file, render_template, Response
import io
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import os
import json

app = Flask(__name__)

def initialize_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def open_url(driver, url):
    print(f"Opening URL: {url}")
    driver.get(url)
    time.sleep(3)

def detect_parcel_links_and_owners(driver, excluded_links):
    soup = BeautifulSoup(driver.page_source, 'lxml')
    parcel_data = []
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 4:
            continue
        link = cells[1].find('a')
        if not link:
            continue
        parcel_number = link.text.strip()
        if parcel_number in excluded_links or ":" in link['href']:
            continue
        owner_name = cells[3].text.strip() if len(cells) > 3 else "N/A"
        parcel_data.append((parcel_number, owner_name))
    print(f"Detected {len(parcel_data)} parcel links with owners.")
    return parcel_data

def navigate_to_parcel_and_subpage(driver, parcel_number):
    try:
        print(f"Navigating to main parcel link: {parcel_number}")
        parcel_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{parcel_number}')]"))
        )
        parcel_link.click()
        print(f"Main parcel link clicked: {parcel_number}")
        print(f"Current URL after first click: {driver.current_url}")
        print(f"Navigating to nested link for parcel: {parcel_number}")
        nested_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{parcel_number}')]"))
        )
        nested_link.click()
        print(f"Nested link clicked for parcel: {parcel_number}")
        print(f"Current URL after nested click: {driver.current_url}")
        if not WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, 'lblLocation'))
        ):
            print(f"Anomalous page detected for parcel: {parcel_number}")
            return False
        print(f"Location Address element found for parcel: {parcel_number}")
        return True
    except Exception as e:
        print(f"Error navigating to parcel {parcel_number}: {e}")
        return False

def scrape_parcel_data(driver):
    try:
        soup = BeautifulSoup(driver.page_source, 'lxml')
        location_element = soup.find('span', {'id': 'lblLocation'})
        location_address = location_element.text.strip() if location_element else "N/A"
        print(f"Scraped Location Address: {location_address}")
        first_floor = soup.find('span', {'id': 'lblFirstFloor'})
        second_floor = soup.find('span', {'id': 'lblSecondFloor'})
        first_sqft = int(first_floor.text.strip()) if first_floor and first_floor.text.strip().isdigit() else 0
        second_sqft = int(second_floor.text.strip()) if second_floor and second_floor.text.strip().isdigit() else 0
        total_sqft = first_sqft + second_sqft
        print(f"First Floor Sq Ft: {first_sqft}, Second Floor Sq Ft: {second_sqft}, Total Sq Ft: {total_sqft}")
        return [location_address, total_sqft]
    except Exception as e:
        print(f"Error scraping parcel data: {e}")
        return None

def write_to_csv(data):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Parcel", "Owner 1", "Address", "Square Footage"])
    writer.writerows(data)
    output.seek(0)
    return output

def scrape_parcels(base_url, excluded_links):
    driver = initialize_driver()
    scraped_data = []
    try:
        open_url(driver, base_url)
        parcel_data = detect_parcel_links_and_owners(driver, excluded_links)
        if not parcel_data:
            yield json.dumps({"progress": 0, "total": 0, "status": "No parcels found"})
            return
        total = len(parcel_data)
        yield json.dumps({"progress": 0, "total": total, "status": "Starting"})
        for i, (parcel_number, owner_name) in enumerate(parcel_data, 1):
            print(f"Processing parcel: {parcel_number}")
            yield json.dumps({"progress": i, "total": total, "status": f"Processing {parcel_number}"})
            success = navigate_to_parcel_and_subpage(driver, parcel_number)
            if not success:
                print(f"Skipping parcel: {parcel_number}")
                driver.back()
                time.sleep(2)
                continue
            data = scrape_parcel_data(driver)
            if data:
                scraped_data.append([parcel_number, owner_name] + data)
            driver.back()
            driver.back()
            time.sleep(2)
        yield json.dumps({"progress": total, "total": total, "status": "Done", "data": scraped_data})
    finally:
        print("Closing the browser.")
        driver.quit()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scrape', methods=['POST'])
def scrape():
    parent_url = request.form['parent_url']
    exclude_str = request.form['exclude_parcels']
    excluded = [x.strip() for x in exclude_str.split(',') if x.strip()]
    filename = request.form['filename']
    if not filename.endswith('.csv'):
        filename += '.csv'

    def generate():
        for event in scrape_parcels(parent_url, excluded):
            yield f"data: {event}\n\n"
        scraped_data = json.loads(event).get("data", [])
        if scraped_data:
            yield f"data: {json.dumps({'status': 'download', 'filename': filename})}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/download/<filename>')
def download(filename):
    scraped_data = request.args.get('data', '')
    if not scraped_data:
        return "No data", 400
    scraped_data = json.loads(scraped_data)
    output = write_to_csv(scraped_data)
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))