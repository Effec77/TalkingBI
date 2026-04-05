from rapidfuzz import process

def correct_tokens(query: str, vocab: list, threshold=85) -> str:
    tokens = query.split()
    corrected = []

    for t in tokens:
        if len(t) < 3 or t.isnumeric():
            corrected.append(t)
            continue

        res = process.extractOne(t.lower(), vocab)
        if res:
            match, score, _ = res
            if score >= threshold:
                corrected.append(match)
            else:
                corrected.append(t)
        else:
            corrected.append(t)

    return " ".join(corrected)
