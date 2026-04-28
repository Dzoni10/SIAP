import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM,Dense,Dropout,GRU
from sklearn.metrics import mean_absolute_percentage_error,mean_absolute_error,mean_squared_error
import matplotlib.pyplot as plt
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.layers import Bidirectional, BatchNormalization


# učitavanje
files = ['meteo_podaci_2021.xlsx','meteo_podaci_2022.xlsx','meteo_podaci_2023.xlsx','meteo_podaci_2024.xlsx']
df_list = []

for f in files:
    temp_df = pd.read_excel(f)
    df_list.append(temp_df)

df = pd.concat(df_list,ignore_index=True)

# timestamp
sati = df["Cas"].astype(str).str.split(':').str[0].astype(int)
df['Datetime'] = pd.to_datetime(df["Datum"] + pd.to_timedelta(sati,unit='h'))
df.set_index('Datetime',inplace=True)
df.sort_index(inplace=True)


#DODAVANJE LAGOVA
#df['lag_2h'] = df['br_bicikala'].shift(2)       #kratkorocni trend
df['lag_24h'] = df['br_bicikala'].shift(24)      #broj bicikala pre dan
#df['lag_48h'] = df['br_bicikala'].shift(48)       #trend pre 2 dana
df['lag_7dana'] = df['br_bicikala'].shift(168)    #broj bickala u isto vreme pre 7 dana


#rupe izbacivanje praznih vrednosti
df.dropna(inplace=True) 

# dummy varijabe
df = pd.get_dummies(df,columns=["godisnje_doba"],drop_first=True)

delete_columns = ['Datum','Cas']
X = df.drop(columns=delete_columns)
y = df['br_bicikala']


train_mask = df['godina'] < 2024
test_mask = df['godina'] == 2024


# SPLIT POSLE SVEGA
X_train_raw = X[train_mask]
y_train_raw = y[train_mask]

X_test_raw = X[test_mask]
y_test_raw = y[test_mask]

# skaliranje
scalerX = MinMaxScaler()
scalery = MinMaxScaler()

X_train_scaled = scalerX.fit_transform(X_train_raw)
y_train_scaled = scalery.fit_transform(y_train_raw.values.reshape(-1,1))

X_test_scaled = scalerX.transform(X_test_raw)
y_test_scaled = scalery.transform(y_test_raw.values.reshape(-1,1))


# sekvence
def create_sequences(X_data,y_data ,time_steps):
    X_seq, y_seq = [], []
    for i in range(len(X_data) - time_steps):
        X_seq.append(X_data[i:(i+time_steps)])
        y_seq.append(y_data[i+time_steps])
    return np.array(X_seq), np.array(y_seq)

time_steps = 24

X_train, y_train = create_sequences(X_train_scaled,y_train_scaled,time_steps)
X_test, y_test = create_sequences(X_test_scaled,y_test_scaled,time_steps)

print (f"Oblik X_train:{X_train.shape}")


def build_model(model_type='LSTM'):
    model = Sequential()

    if model_type == 'LSTM':
        model.add(Bidirectional(LSTM(128,activation='relu',return_sequences=True),input_shape=(X_train.shape[1],X_train.shape[2])))
        model.add(BatchNormalization())
        model.add(Dropout(0.2))
        model.add(Bidirectional(LSTM(64,activation='relu',return_sequences=False)))
        model.add(BatchNormalization())
    elif model_type == 'GRU':
        model.add(Bidirectional(GRU(128,activation='relu',return_sequences=True),input_shape=(X_train.shape[1],X_train.shape[2])))
        model.add(BatchNormalization())
        model.add(Dropout(0.2))
        model.add(Bidirectional(GRU(64,activation='relu',return_sequences=False)))
        model.add(BatchNormalization())
    
    model.add(Dropout(0.2))
    model.add(Dense(32,activation='relu'))
    model.add(Dense(1)) #izlazni sloj

    opt = Adam(learning_rate=0.0005)
    model.compile(optimizer=opt, loss='mae', metrics=['mae'])
    return model

model_lstm = build_model(model_type='LSTM')
model_lstm.summary()



#Trening i evalucaija

early_stop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

# Treniranje modela
history = model_lstm.fit(
    X_train, y_train,
    epochs=50,
    batch_size=32,
    validation_split=0.1, 
    callbacks=[early_stop],
    verbose=1
)

y_pred_scaled = model_lstm.predict(X_test)


y_pred = scalery.inverse_transform(y_pred_scaled)
y_test_real = scalery.inverse_transform(y_test)

rmse = np.sqrt(mean_squared_error(y_test_real,y_pred))
mae = mean_absolute_error(y_test_real, y_pred)
mape = mean_absolute_percentage_error(y_test_real,y_pred)

print("Test RMSE: ", rmse)
print("Test MAE: ", mae)

plt.figure(figsize=(10,5))
plt.plot(history.history['loss'],label='Trening Loss (MAE)')
plt.plot(history.history['val_loss'],label='Validation Loss (MAE)')
plt.title('Model learning history (Loss)')
plt.xlabel('Epoch')
plt.ylabel('Loss (scaled)')
plt.legend()
plt.show()


show_from=1000 # prikaz 200 sati
show_to=1200

plt.figure(figsize=(15,7))
plt.plot(y_test_real[show_from:show_to],label='Real number of bicycles',color='blue',alpha=0.7)
plt.plot(y_pred[show_from:show_to],label='Prediction model',color='red',alpha=0.8,linestyle='--')
plt.title('Difference show between Real vs Prediced number of bicycles (per hour)')
plt.xlabel('Hours')
plt.ylabel('Number of bicycles')
plt.legend()
plt.grid(True)
plt.show()


# DNEVNI NIVO

# 1. Uzimamo originalni vremenski indeks iz testnog skupa.
# Pošto smo koristili klizeći prozor (time_steps = 24), 
# naše predikcije počinju tek od 24. sata u testnom skupu.
dates_test = y_test_raw.index[time_steps:]

# 2. Pravimo novi DataFrame koji spaja tačno vreme, stvarne i predviđene vrednosti.
# Koristimo .flatten() jer su y_test_real i y_pred 2D nizovi (n, 1), a Pandas traži 1D niz (n,).
results_df = pd.DataFrame({
    'Real_hour': y_test_real.flatten(),
    'Predicted_hour': y_pred.flatten()
}, index=dates_test)

# 3. Resamplovanje na dnevni nivo ('D' označava Day). 
# Ovo automatski pronalazi sve sate u jednom danu i sumira ih.
daily_results = results_df.resample('D').sum()
 
# 4. Računanje metrika za dnevni nivo
daily_rmse = np.sqrt(mean_squared_error(daily_results['Real_hour'], daily_results['Predicted_hour']))
daily_mae = mean_absolute_error(daily_results['Real_hour'], daily_results['Predicted_hour'])

print("\n--- DAILY RESULTS ---")
print(f"Daily RMSE: {daily_rmse:.2f} bicycles")
print(f"Daily MAE: {daily_mae:.2f} bicycles")

# Opciono: Crtanje grafika za dnevni nivo (jako korisno za izveštaj)
plt.figure(figsize=(12, 5))
plt.plot(daily_results.index, daily_results['Real_hour'], label='Real daily number', color='blue', alpha=0.7)
plt.plot(daily_results.index, daily_results['Predicted_hour'], label='Predicted daily number', color='red', linestyle='--', alpha=0.8)
plt.title('Daily number on street Bulevar oslobodjenja (2024. year)')
plt.xlabel('Date')
plt.ylabel('Total number of bicycles')
plt.legend()
plt.grid(True)
plt.show()