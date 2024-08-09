import re

def find_uppercase_words(text):
    """
    查找文本中全部为大写字母的单词。

    Args:
        text: 要搜索的文本。

    Returns:
        一个包含所有大写单词的列表。
    """
    pattern = r'\b[A-Z]+\b'
    matches = re.findall(pattern, text)
    return matches