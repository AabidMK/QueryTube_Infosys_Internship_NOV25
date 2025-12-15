# upsert_lancedb.py (FINAL FIXED VERSION for your LanceDB)
import ast
import pandas as pd
import numpy as np
import lancedb
import pyarrow as pa
from sentence_transformers import SentenceTransformer
import os
import sys
EMBEDDING_DIM=384

# ---------- CONFIG ----------
CSV_PATH = "kaggle_newembeddings1.csv"   # change if needed
DB_FOLDER = "lancedb_videos"
TABLE_NAME = "videos"
EMBEDDING_COL = "embedding"
MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 256
# ----------------------------


def parse_embedding(e):
    if pd.isna(e):
        return None
    if isinstance(e, (list, tuple, np.ndarray)):
        return list(e)
    if isinstance(e, str):
        s = e.strip()
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple, np.ndarray)):
                return [float(x) for x in parsed]
        except:
            pass
        if "," in s:
            try:
                return [float(x) for x in s.split(",")]
            except:
                pass
    return None


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def main():
    ensure_dir(DB_FOLDER)

    # Connect DB
    db = lancedb.connect(DB_FOLDER)

    # -------------------------------
    # ✅ FIXED: Schema required by your LanceDB version
    # -------------------------------

    schema = pa.schema([
        ("__id__", pa.string()),
        (
            "embedding",
            pa.list_(
                pa.float32(),
                list_size=EMBEDDING_DIM   # ✅ THIS makes it a vector
            )
        ),
        ("payload", pa.struct([
            ("video_id", pa.string()),
            ("transcript", pa.string()),
            ("title", pa.string()),
            ("channel_title", pa.string()),
            ("view_count", pa.int64()),
            ("duration", pa.string()),
        ]))
    ])

    # Create or open table
    if TABLE_NAME in db.table_names():
        tbl = db.open_table(TABLE_NAME)
    else:
        tbl=db.create_table(TABLE_NAME,schema=schema,mode="overwrite")
    # -------------------------------


    # Load CSV
    try:
        df = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        print(f"CSV not found: {CSV_PATH}")
        sys.exit(1)

    required = {"video_id","combined_text","embedding","title","channel_title","viewCount","duration"}
    if not required.issubset(df.columns):
        print("CSV missing some required columns!")
        sys.exit(1)

    # Parse Embeddings
    parsed_embeddings = []
    dim = None

    for e in df[EMBEDDING_COL].values:
        vec = parse_embedding(e)
        parsed_embeddings.append(vec)
        if vec is not None and dim is None:
            dim = len(vec)

    # Load model if needed
    model = None
    if dim is None:
        print("No embeddings found. Generating using SentenceTransformer...")
        model = SentenceTransformer(MODEL_NAME)
        dim=model.get_sentence_embedding_dimension()

    # UPSERT rows
    rows = []

    for idx, row in df.iterrows():
        vid = str(row["video_id"])
        vec = parsed_embeddings[idx]

        if vec is None and model:
            text = str(row["transcript"]) if pd.notna(row["transcript"]) else ""
            if text.strip():
                vec = model.encode(text).tolist()
        if vec is None:
            continue
        if len(vec)!=EMBEDDING_DIM:
            if len(vec)>EMBEDDING_DIM:
                vec=vec[:EMBEDDING_DIM]
            else:
                vec=vec +[0.0] * (EMBEDDING_DIM-len(vec))

        payload = {
            "video_id": str(row["video_id"]),
            "transcript": str(row["combined_text"]),   # FIXED
            "title": str(row["title"]),
            "channel_title": str(row["channel_title"]),
            "view_count": int(row["viewCount"]) if pd.notna(row["viewCount"]) else None,   # FIXED
            "duration": str(row["duration"]),         # You have this
        }
        if not isinstance(vec,list):
            continue
        if len(vec)!=EMBEDDING_DIM:
            continue

        rows.append({
            "__id__": vid,
            #Must match schema
            "embedding": vec,
            #must match schema
            "payload": payload
            #Must match schema
        })

        if len(rows) >= BATCH_SIZE:
            tbl.add(rows)
            print(f"Inserted {len(rows)} rows...")
            rows = []

    if rows:
        tbl.add(rows)
        print(f"Inserted final {len(rows)} rows.")

    print("\nTotal rows in LanceDB table:", len(tbl))

    # Very small test search
    arrow_table=tbl.to_arrow()
    sample_row = arrow_table.to_pylist()[0]
    #print("sample row:,sample_row")
    query_vec = sample_row["embedding"]
    print(type(query_vec),len(query_vec))
    results = tbl.search(query_vec,vector_column_name="embedding").limit(5).to_list()
    print("\nTop 5 Similar Results:\n")
    for r in results:
        payload = r["payload"]
        print({

            "video_id": payload["video_id"],
            "title": payload["title"],
            "channel_title": payload["channel_title"],
            "score": r["_distance"]
        })
    
if __name__ == "__main__":
    main()