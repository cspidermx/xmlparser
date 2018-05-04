import os

for f in os.listdir('.'):
    if f[-3:] == 'xml':
        print(f)
