import os

output_file = "bot_final.txt"
this_file = os.path.basename(__file__)

# Папки, которые исключаются из обхода
ignored_dirs = {".venv", "External Libraries", "Scratches and Consoles", "__pycache__"}

# Очистка выходного файла
with open(output_file, "w", encoding="utf-8") as f:
    pass

# Сборка кода из всех подходящих .py файлов
with open(output_file, "w", encoding="utf-8") as out:
    for root, dirs, files in os.walk("."):
        # Исключаем системные папки
        dirs[:] = [d for d in dirs if d not in ignored_dirs]

        for file in sorted(files):
            if file.endswith(".py") and file != this_file:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, ".")
                out.write(f"# {rel_path}\n\n")
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        out.write(content)
                        out.write("\n\n\n")
                except Exception as e:
                    out.write(f"# Ошибка чтения файла {rel_path}: {e}\n\n")



