import pandas as pd

df = pd.read_csv("IOT-temp.csv")

df = df[df["out/in"] == "In"]
df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
df["noted_date"] = df["noted_date"].dt.date

p05 = df["temp"].quantile(0.05)
p95 = df["temp"].quantile(0.95)
clean = df[(df["temp"] >= p05) & (df["temp"] <= p95)]

daily = clean.groupby("noted_date", as_index=False)["temp"].mean()
hot = daily.nlargest(5, "temp")
cold = daily.nsmallest(5, "temp")

print("5 самых жарких дней:")
print(hot)

print("5 самых холодных дней:")
print(cold)
