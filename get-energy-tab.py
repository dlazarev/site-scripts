from influxdb import InfluxDBClient
import calendar
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


def get_dates_array():
    now = datetime.now()
    today = date(now.year, now.month, now.day)
    last_days = []
    for i in range(1,13):
        curr_date = today + relativedelta(months=-i)
        curr_date = date(curr_date.year, curr_date.month, calendar.monthrange(curr_date.year, curr_date.month)[1]) 
        last_days.append((curr_date, str(curr_date) + 'T' + "20:55:00Z", str(curr_date) + 'T' + "21:05:00Z"))
    return last_days

def find_last_energy_value(res_of_query, measurement):
    curr_energy = 100000.0
    for point in res_of_query.get_points(measurement=measurement):
        if curr_energy < point['energy']:
            return point
        curr_energy = point['energy']
    

def main():
    client_influx = InfluxDBClient("perets.su")
    client_influx.switch_database("electrical")

    """
    Получаем данные с pzem-004
    """
    phases = ['phase1', 'phase2', 'phase3']
    result_table = {}

    for d in get_dates_array():
        query = f"select energy from phase1,phase2,phase3 where time > '{d[1]}' and time <= '{d[2]}' order by time desc limit 50"
        res = client_influx.query(query)
        date_key =str(calendar.month_name[d[0].month]) + ' ' + str(d[0].year)
        result_table[date_key] = []
        for phase in phases:
            result_table[date_key].append(find_last_energy_value(res, phase)['energy'])
        result_table[date_key].append(round(sum(result_table[date_key][0:3]), 1))

    """
    Получаем данные счетчика на столбе
    """
    res_of_query = client_influx.query("select time, value from ElectricityMeterReader order by time desc limit 13")
    last_value = -1
    for point in res_of_query.get_points():
        if last_value > 0:
            curr_value = last_value - point["value"]
            d = point["time"].split('-')[:2]
            date_key = str(calendar.month_name[int(d[1])]) + ' ' + d[0]
            result_table[date_key].append(curr_value)
        last_value = point["value"]

    for key, val in result_table.items():
        print(key, val)

if __name__ == "__main__":
    main()