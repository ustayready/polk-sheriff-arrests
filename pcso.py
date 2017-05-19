'''
Script Name: pcso.py
Version: 1
Revised Date: 03/23/2017
Python Version: 3
Description: Downloads all the arrests from 1990-2017 from Polk County Sheriffs Office
Copyright: 2017 Mike Felch <mike@linux.edu> 
URL: http://www.forensicpy.com/
--
- ChangeLog -
v1 - [03-23-2017]: Original code
'''

from core.booking_entry import BookingEntry
from bs4 import BeautifulSoup
from datetime import date, timedelta as td, datetime
from threading import Thread
import requests, re, queue

SEARCH_URL = 'http://www.polksheriff.org/inq/Pages/Jail.aspx'
BOOKING_URL = 'http://www.polksheriff.org/inq/pages/inmate.aspx?BookingNumber='
THEAD_COUNT = 50

def main():
    date_queue = queue.Queue()

    start = datetime.now()
    print("[!] Started processing at: {}".format(start))

    view_state = refresh_viewstate()
    dates = get_dates('1990-01-01', '2017-01-03')

    for arrest_date in dates:
        date_queue.put(arrest_date)

    for thread_id in range(THEAD_COUNT):
        worker = Thread(target=process_arrests, args=(thread_id, view_state, date_queue,))
        worker.setDaemon(True)
        worker.start()

    print('- Waiting on workers to complete...')
    date_queue.join()

    end = datetime.now()
    print("[!] Ended processing at: {}".format(end))

def process_arrests(thread_id, view_state, dates):
    while True:
        arrest_date = dates.get()

        print('- Thread #{}: Capturing arrests for: {}'.format(thread_id, arrest_date))
        arrests = capture_arrests(view_state, arrest_date.month, arrest_date.day, arrest_date.year)

        if arrests is None:
            print('- Adding arrest date back into queue: {}'.format(arrest_date))
            dates.put(arrest_date)
        else:
            save_arrests(arrests,arrest_date)
            dates.task_done()

def save_arrests(arrests, arrest_date):
    save_file = 'data/arrests_{}-{}-{}.csv'.format(arrest_date.year, arrest_date.month, arrest_date.day)
    with open(save_file,'w') as fh:
        header = [
            'number', 'last_name', 'middle_name', 'first_name', 'race', 'sex',
            'dob', 'booking_date', 'release_date', 'location'
        ]

        fh.write(','.join(header)+'\n')
        for arrest in arrests:
            line = '{}\n'.format(arrest)
            fh.write(line)

def get_dates(start_date, end_date):
    dates = []

    start = [int(x) for x in start_date.split('-')]
    end = [int(x) for x in end_date.split('-')]

    d1 = date(start[0], start[1], start[2])
    d2 = date(end[0], end[1], end[2])

    for i in range((d2-d1).days + 1):
        dates.append(d1 + td(days=i))

    return dates

def refresh_viewstate():
    parms = {}
    response = requests.get(SEARCH_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    search_form = soup.find('form',id='aspnetForm')
    inputs = search_form.findAll('input')

    required_fields = ['__VIEWSTATE','__VIEWSTATEGENERATOR','__EVENTVALIDATION']
    for input in inputs:
        if input.get('id') in required_fields:
            parms[input.get('id')] = input.get('value')

    return parms

def capture_arrests(parms, month, day, year):
    arrests = []

    parms['ctl00$ctl15$g_413cdd9d_e152_40ad_9a7a_a595a01d2d51$ctl00$ddlBookingMonth'] = str(month)
    parms['ctl00$ctl15$g_413cdd9d_e152_40ad_9a7a_a595a01d2d51$ctl00$ddlBookingDay'] = str(day)
    parms['ctl00$ctl15$g_413cdd9d_e152_40ad_9a7a_a595a01d2d51$ctl00$ddlBookingYear'] = str(year)
    parms['ctl00$ctl15$g_413cdd9d_e152_40ad_9a7a_a595a01d2d51$ctl00$btnBookingDateSearch'] = 'Search'

    try:
        search = requests.post(SEARCH_URL, data=parms)
        soup = BeautifulSoup(search.content, 'html.parser')
        grid = soup.find('table', id=re.compile("_grdResults"))

        for row in grid.findAll('tr'):
            cols = [x.text for x in row.findAll('td')]

            if len(cols) > 0:
                try:
                    be = BookingEntry()
                    be.booking_number = cols[0]

                    name = cols[1]
                    name_parts = [x.strip() for x in name.split(',')]
                    mid_parts = name_parts[1].split(' ', 1)
                    be.first_name = name_parts[0]
                    be.middle_name = mid_parts[1] if len(mid_parts) > 1 else ''
                    be.last_name = mid_parts[0]

                    race_sex = cols[2]
                    be.race = race_sex[:len(race_sex) // 2]
                    be.sex = race_sex[len(race_sex) // 2:]

                    be.dob = cols[3]
                    be.booking_date = cols[4]
                    be.release_date = cols[5]
                    be.location = cols[6]

                    arrests.append(be)
                except Exception as ex:
                    pass
    except Exception as ex:
        return None

    return arrests

if __name__ == '__main__':
    main()
