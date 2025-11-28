import re

refFile = 'raw_271125_reference.csv'
newFile = 'elonmusk_db.csv'

with open(refFile, 'r', encoding='utf-8') as f:
    content1 = f.read()

with open(newFile, 'r', encoding='utf-8') as f:
    content2 = f.read()

pattern = r'\d{19}'
matches1 = re.findall(pattern, content1)
matches2 = re.findall(pattern, content2)
count1 = len(matches1)
count2 = len(matches2)

print(f"Found {count1} occurrences of 19-digit numbers in {refFile}")
print(f"Found {count2} occurrences of 19-digit numbers in {newFile}")
