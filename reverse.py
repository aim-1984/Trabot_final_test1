import os

def parse_bot_final(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    file_blocks = {}
    current_file = None
    current_lines = []

    for line in lines:
        if line.startswith("# ") and line.strip().endswith(".py"):
            if current_file:
                file_blocks[current_file] = ''.join(current_lines).strip() + '\n'
            current_file = line.strip()[2:]  # удаляем "# " в начале
            current_lines = []
        else:
            if current_file:
                current_lines.append(line)

    if current_file and current_lines:
        file_blocks[current_file] = ''.join(current_lines).strip() + '\n'

    return file_blocks


def write_files_from_blocks(blocks):
    for file_path, content in blocks.items():
        dir_name = os.path.dirname(file_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Перезаписан файл: {file_path}")


if __name__ == "__main__":
    bot_final_path = "bot_final.txt"
    if not os.path.exists(bot_final_path):
        print("❌ Файл bot_final.txt не найден.")
    else:
        file_blocks = parse_bot_final(bot_final_path)
        write_files_from_blocks(file_blocks)
        print("🎉 Все файлы успешно восстановлены из bot_final.txt")
