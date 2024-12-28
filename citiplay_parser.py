import os
import re
import imaplib
import email
from datetime import datetime
from email.policy import default
import PyPDF3
from PyPDF3.pdf import BytesIO
from elasticsearch import Elasticsearch
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

GMAIL_USERNAME = config['GMAIL']['Username']
GMAIL_PASSWORD = config['GMAIL']['Password']
ES_HOST = config['ELASTICSEARCH']['Host']
ES_USERNAME = config['ELASTICSEARCH']['Username']
ES_PASSWORD = config['ELASTICSEARCH']['Password']

# Connect to Gmail
def connect_to_gmail():
    imap_server = "imap.gmail.com"
    imap = imaplib.IMAP4_SSL(imap_server)
    imap.login(GMAIL_USERNAME, GMAIL_PASSWORD)
    return imap

# Fetch PDF attachments from Gmail
def fetch_pdf_attachments(mail, sender_email):
    mail.select('inbox')
    _, message_numbers = mail.search(None, f'FROM "{sender_email}"')
    pdf_files = []

    for message_number in message_numbers[0].split():
        _, msg_data = mail.fetch(message_number, '(RFC822)')
        msg = email.message_from_bytes(msg_data[0][1], policy=default)

        for part in msg.iter_parts():
            if part.get_content_type() == 'application/pdf':
                pdf_files.append(part.get_payload(decode=True))
    return pdf_files

# Extract player name and position
def extract_name_and_position(text):
    match = re.search(r'Work Rate \(m/min\) \n([\d.]+|N\/A)\n([^\n]+)\n(.*?)\nDATE', text, re.DOTALL)
    if match:
        return match.group(2).strip(), match.group(3).strip()
    return None, None

# Create timestamp
def create_timestamp(date_str, time_str):
    full_date_str = f"2024 {date_str} {time_str}"
    timestamp = datetime.strptime(full_date_str, "%Y %b %d %H:%M")
    return timestamp.isoformat(), timestamp.strftime("%A")

# Parse match report
def parse_match_report(text):
    match_report_match = re.search(r'Match\s*Report\s*(.*?)\s*(\d+)\s*:\s*(\d+)\s*(.*?)\s*Technical Summary', text, re.DOTALL)
    if match_report_match:
        team_1 = match_report_match.group(1).strip()
        score_1 = int(match_report_match.group(2))
        score_2 = int(match_report_match.group(3))
        team_2 = match_report_match.group(4).strip()
    else:
        team_1 = team_2 = score_1 = score_2 = None

    date_match = re.search(r'DATE\s*(\w+\s*\d+)', text)
    date = date_match.group(1) if date_match else None

    playing_time_match = re.search(r'PLAYING\s*TIME\s*(\d+)\s*Min', text)
    playing_time = int(playing_time_match.group(1)) if playing_time_match else None

    ball_possessions = re.search(r'(\d+)\s*Ball Possessions \(\#\)', text)
    one_touch = re.search(r'(\d+)\s*One-Touch \(\#\)', text)
    short_possessions = re.search(r'(\d+)\s*Short Possessions \(\#\)', text)
    long_possessions = re.search(r'(\d+)\s*Long Possessions \(\#\)', text)
    total_releases = re.search(r'(\d+)\s*Total Releases \(\#\)', text)

    distance_covered = re.search(r'(\d+\.\d+)\s*Distance Covered \(km\)', text)
    sprint_distance = re.search(r'(\d+\.\d+)\s*Sprint Distance \(m\)', text)
    accl_decl = re.search(r'(\d+)\s*Accl/Decl \(\#\)', text)
    work_rate = re.search(r'(\d+\.\d+)\s*Work Rate \(m/min\)', text)

    player_name, position = extract_name_and_position(text)
    timestamp = None
    if date:
        try:
            parsed_date = datetime.strptime(date, '%b %d')
            timestamp = parsed_date.replace(year=datetime.now().year).isoformat()
        except ValueError:
            pass

    return {
        "_index": "cityplay_match",
        "vs_team": team_2,
        "my_team": team_1,
        "score": f"{score_1} - {score_2}",
        "@timestamp": timestamp,
        "_id": f"{player_name}_{timestamp}_{team_1}_{team_2}_{score_1}_{score_2}" if player_name and timestamp else None,
        'player_name': player_name,
        'position': position,
        'date': date,
        'playing_time_minutes': playing_time,
        'ball_possessions': int(ball_possessions.group(1)) if ball_possessions else None,
        'one_touch': int(one_touch.group(1)) if one_touch else None,
        'short_possessions': int(short_possessions.group(1)) if short_possessions else None,
        'long_possessions': int(long_possessions.group(1)) if long_possessions else None,
        'total_releases': int(total_releases.group(1)) if total_releases else None,
        'distance_covered_km': float(distance_covered.group(1)) if distance_covered else None,
        'sprint_distance_m': float(sprint_distance.group(1)) if sprint_distance else None,
        'accl_decl_count': int(accl_decl.group(1)) if accl_decl else None,
        'work_rate_m_per_min': float(work_rate.group(1)) if work_rate else None,
    }

# Initialize Elasticsearch connection
def init_elasticsearch():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USERNAME, ES_PASSWORD),
        verify_certs=False
    )
    print(es.info())
    return es

if __name__ == '__main__':
    es_connection = init_elasticsearch()

    mail = connect_to_gmail()
    sender_email = config['GMAIL']['SenderEmail']
    pdf_files = fetch_pdf_attachments(mail, sender_email)

    for pdf_content in pdf_files:
        data = parse_match_report(pdf_content)
        if data:
            print(data)
            try:
                if es_connection.exists(index=data['_index'], id=data['_id']):
                    print(f"Document with ID {data['_id']} already exists. Skipping indexing.")
                else:
                    es_connection.index(index=data['_index'], id=data['_id'], document=data)
            except Exception as e:
                print(f"Failed to index document with ID {data['_id']}:", e)
