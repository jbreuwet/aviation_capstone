import httpx, json
response = httpx.get('https://opensky-network.org/api/states/all?lamin=25.0&lomin=-125.0&lamax=50.0&lomax=-65.0')
print('Status:', response.status_code)
data = response.json()
print('Keys:', data.keys())
print('First state vector:', data['states'][0])
print('Total aircraft:', len(data['states']))