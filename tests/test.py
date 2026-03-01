import requests

url = "https://api.ouro.foundation/routes/mmoderwell/generate"
headers = {
    "Authorization": "ApiKey 5f4506989f81237700d8047b6a91564f726d8982944c2016c2b4d379db37c2a67511b9dcf7c3c957ede2b699b7e4c79fc33b48f204910e00f46bb7dab6730e8d",
    "Content-Type": "application/json",
}

payload = {"composition": "FeNi", "temperature": 0.8, "max_new_tokens": 3000}

response = requests.post(url, headers=headers, json=payload)

print(response.status_code)
print(response.json())
