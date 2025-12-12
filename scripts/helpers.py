import re

def split_text_into_chunks(text, max_chars=1200, overlap_chars=300):
    text = re.sub(r'\s+', ' ', str(text)).strip()
    if not text:
        return []
    chunks, start, L = [], 0, len(text)
    while start < L:
        end = min(start + max_chars, L)
        chunks.append(text[start:end].strip())
        if end == L:
            break
        start = max(0, end - overlap_chars)
    return chunks
