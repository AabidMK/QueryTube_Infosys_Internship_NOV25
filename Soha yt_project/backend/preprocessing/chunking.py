def chunk_text(text, chunk_size=400):
    """
    Break a long transcript text into smaller chunks.
    
    Why chunk?
    - Embedding models have token limits.
    - Smaller chunks give better search accuracy.
    
    chunk_size = number of WORDS per chunk (default = 400)
    """

    if not text or not isinstance(text, str):
        return []

    words = text.split()
    chunks = []

    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)

    return chunks


# ------------ TESTING PURPOSE ONLY ----------------
if __name__ == "__main__":
    sample = "This is demo text " * 200    # fake long text
    out = chunk_text(sample)

    print(f"Total chunks created = {len(out)}")
    print("\nExample chunk:\n")
    print(out[0])
