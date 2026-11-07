"""Example eval case for the exec executor.

Contract: read one JSON object from stdin
    {"case_id": str, "payload": {...}, "attempt": int}
and print one JSON object on stdout
    {"score": float, "passed": bool, "metrics": {name: number}}
then exit 0. Exit code 3 (or {"error": ..., "retryable": false}) means do not retry.

This case scores a predicted axis-aligned 3D box against a reference box by
volumetric IoU. Standard library only.
"""
import json
import sys


def volume(b):
    return max(0.0, b[3] - b[0]) * max(0.0, b[4] - b[1]) * max(0.0, b[5] - b[2])


def iou(a, b):
    inter = [max(a[0], b[0]), max(a[1], b[1]), max(a[2], b[2]),
             min(a[3], b[3]), min(a[4], b[4]), min(a[5], b[5])]
    vi = volume(inter)
    union = volume(a) + volume(b) - vi
    return vi / union if union > 0 else 0.0


def main():
    job = json.load(sys.stdin)
    p = job["payload"]
    try:
        pred, ref = p["pred"], p["ref"]
        assert len(pred) == 6 and len(ref) == 6
    except (KeyError, AssertionError):
        print(json.dumps({"error": "payload needs pred and ref as [x0,y0,z0,x1,y1,z1]", "retryable": False}))
        return
    score = iou(pred, ref)
    print(json.dumps({
        "score": score,
        "passed": score >= p.get("threshold", 0.9),
        "metrics": {"iou": score, "volume_rel_err": abs(volume(pred) - volume(ref)) / max(volume(ref), 1e-12)},
    }))


if __name__ == "__main__":
    main()
