import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import holidays

# 1. Podešavanje API klijenta
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# 2. Parametri (Novi Sad)
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 45.250787,
    "longitude": 19.837743,
    "start_date": "2021-01-01",
    "end_date": "2024-12-31",
    "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "shortwave_radiation", "precipitation", "snow_depth", "snowfall"],
    "timezone": "Europe/Berlin"
}

responses = openmeteo.weather_api(url, params=params)
res = responses[0]

hourly = res.Hourly()

hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left" 
    ),
    "temperatura": hourly.Variables(0).ValuesAsNumpy(),
    "vlaznost": hourly.Variables(1).ValuesAsNumpy(),
    "brzina_vetra": hourly.Variables(2).ValuesAsNumpy(),
    "insolacija": hourly.Variables(3).ValuesAsNumpy(),
    "padavine": hourly.Variables(4).ValuesAsNumpy(),
    "sneg_U": hourly.Variables(5).ValuesAsNumpy(),
    "sneg_N": hourly.Variables(6).ValuesAsNumpy()
}

df = pd.DataFrame(data=hourly_data)

# Čišćenje vremenske zone
df['date'] = df['date'].dt.tz_convert('Europe/Belgrade').dt.tz_localize(None)

# --- 3. REŠAVANJE DATUMA I SATA (01:00 do 24:00) ---
# Pomeramo vreme za 1 sat unapred
df['date'] = df['date'] + pd.Timedelta(hours=1)

# Pravimo kolonu Datum: Ako je vreme 00:00, taj sat tehnički pripada kraju prethodnog dana
df['Datum'] = df['date'].apply(lambda dt: dt - pd.Timedelta(days=1) if dt.hour == 0 else dt).dt.normalize()

# Pravimo kolonu Cas: Sve što je 00:00 ispisujemo fiksno kao "24:00"
df['Cas'] = df['date'].apply(lambda dt: "24:00" if dt.hour == 0 else dt.strftime('%H:%M'))

# Brišemo staru 'date' kolonu i ređamo kolone da Datum i Cas budu na početku
df = df.drop(columns=['date'])
ostale_kolone = [col for col in df.columns if col not in ['Datum', 'Cas']]
df = df[['Datum', 'Cas'] + ostale_kolone]


# --- 4. Dodatne kolone (sada se vežu za novu kolonu Datum) ---
df['godina'] = df['Datum'].dt.year
df['mesec'] = df['Datum'].dt.month
df['dan_u_nedelji'] = df['Datum'].dt.dayofweek + 1
df['is_weekend'] = df['dan_u_nedelji'].apply(lambda x: 1 if x >= 6 else 0)

def get_season(month):
    if month in [12, 1, 2]: return 'zima'
    elif month in [3, 4, 5]: return 'prolece'
    elif month in [6, 7, 8]: return 'leto'
    else: return 'jesen'

df['godisnje_doba'] = df['mesec'].apply(get_season)

# Praznici za Srbiju
rs_holidays = holidays.RS() 
df['is_holiday'] = df['Datum'].apply(lambda x: 1 if x in rs_holidays else 0)


# --- 4a. Zaokruživanje numeričkih vrednosti ---
kolone_za_zaokruzivanje = ['temperatura', 'vlaznost', 'brzina_vetra', 'insolacija', 'padavine', 'sneg_U', 'sneg_N']

for kolona in kolone_za_zaokruzivanje:
    df[kolona] = df[kolona].astype(float).round(2)


# --- 5. Čuvanje u Excel fajlove ---
# Pre čuvanja obezbeđujemo da Excel kolonu Datum tretira isključivo kao čist datum bez sati
df['Datum'] = df['Datum'].dt.date

for godina in df['godina'].unique():
    df_godina = df[df['godina'] == godina]
    file_name = f"meteo_podaci_{godina}.xlsx"
    df_godina.to_excel(file_name, index=False)
    print(f"Uspešno sačuvan: {file_name}")