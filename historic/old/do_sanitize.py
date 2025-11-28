from historic.old.sanitizer import sanitize_csv_to_file

# Option 1: Pass file content as bytes
with open('../elonmusk_db.csv.bak', 'rb') as f:
    content = f.read()

sanitized = sanitize_csv_to_file(content, output_path='sanitized_elonmusk_db.csv')

# # Option 2: Pass file content as string
# with open('elonmusk_db.csv', 'r', encoding='utf-8') as f:
#     content = f.read()
#
# sanitized = sanitize_csv_to_file(content, output_path='sanitized_elonmusk_db.csv')
