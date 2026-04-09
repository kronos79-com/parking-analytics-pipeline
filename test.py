lines = open('scripts/generate_datasets_v2.py').readlines()
for i, line in enumerate(lines, 1):
    if 'psp' in line.lower():
        print(f'{i}: {line}', end='')