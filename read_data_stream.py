import requests

r = requests.get('https://stream.wikimedia.org/v2/stream/page-create', stream=True)

for line in r.iter_lines():
    if line:
        print(line)
