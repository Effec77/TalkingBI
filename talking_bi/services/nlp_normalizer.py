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
            # Guard against destructive over-corrections:
            # e.g., "performance" -> "for" (high fuzzy score but wrong intent).
            length_gap_ok = abs(len(str(match)) - len(t)) <= 2
            if score >= threshold and length_gap_ok:
                corrected.append(match)
            else:
                corrected.append(t)
        else:
            corrected.append(t)

    return " ".join(corrected)
