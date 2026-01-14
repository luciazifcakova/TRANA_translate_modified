#!/usr/bin/env python3
import argparse
import pandas as pd
import sys

VERSION = "v0.0.3"

# Preferred rank order (best label first)
RANK_PRIORITY = [
    "subspecies",
    "species",
    "species subgroup",
    "species group",
    "genus",
    "family",
    "order",
    "class",
    "phylum",
    "clade",
    "superkingdom",
    "kingdom",
    "domain",
]

# Column-name aliases that appear in different dumps/tools
ALIASES = {
    "superkingdom": ["superkingdom", "domain", "kingdom", "superkingdom_name"],
    "phylum": ["phylum", "phylum_name"],
    "class": ["class", "class_name"],
    "order": ["order", "order_name"],
    "family": ["family", "family_name"],
    "genus": ["genus", "genus_name"],
    "species": ["species", "species_name", "scientific_name", "name", "organism_name"],
    "subspecies": ["subspecies", "subspecies_name"],
    "species subgroup": ["species subgroup", "species_subgroup", "species_subgroup_name"],
    "species group": ["species group", "species_group", "species_group_name"],
    "clade": ["clade", "clade_name"],
    "kingdom": ["kingdom", "kingdom_name"],
    "domain": ["domain", "domain_name", "superkingdom"],
}


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_row_accessor(row: pd.Series):
    mapping = {str(k).strip().lower(): k for k in row.index}

    def get(colname: str):
        key = colname.strip().lower()
        if key in mapping:
            return row[mapping[key]]
        return None

    return get


def get_best_tax_label(row: pd.Series) -> str:
    get = build_row_accessor(row)

    for rank in RANK_PRIORITY:
        for candidate in ALIASES.get(rank, [rank]):
            v = get(candidate)
            if v is None:
                continue
            if pd.notna(v) and str(v).strip() != "":
                return str(v).strip()

    return "Unknown"


def find_tax_id_col(taxonomy_df: pd.DataFrame) -> str | None:
    # robust: ignore case + whitespace
    candidates = []
    for c in taxonomy_df.columns:
        key = str(c).strip().lower()
        if key in ("tax_id", "taxid", "tax id", "tax-id"):
            candidates.append(c)

    if candidates:
        return candidates[0]

    # as an extra fallback: if first column looks like tax id column
    # (many taxdump exports put tax_id first)
    if taxonomy_df.shape[1] >= 1:
        c0 = taxonomy_df.columns[0]
        key0 = str(c0).strip().lower()
        if "tax" in key0 and "id" in key0:
            return c0

    return None


def main(tsv_path, taxonomy_path, output_path):
    # Read the per-sample TSV
    df = pd.read_csv(tsv_path, sep="\t", header=0, dtype=str)
    original_headers = [str(c) for c in df.columns.tolist()]

    # Read taxonomy mapping
    taxonomy_df = pd.read_csv(taxonomy_path, sep="\t", dtype=str)
    taxonomy_df = normalize_cols(taxonomy_df)

    tax_id_col = find_tax_id_col(taxonomy_df)

    # If taxonomy table doesn't have tax_id, DO NOT FAIL THE SAMPLE.
    # Just write unchanged headers so the pipeline continues.
    if tax_id_col is None:
        df.to_csv(output_path, sep="\t", index=False)
        print(
            f"WARNING: Could not find tax_id column in taxonomy file; "
            f"writing unchanged output for: {output_path}",
            file=sys.stderr,
        )
        return

    taxonomy_df = taxonomy_df.set_index(tax_id_col, drop=True)

    # Build mapping taxid -> best label
    tax_id_to_label = {}
    for tax_id, row in taxonomy_df.iterrows():
        tid = str(tax_id).strip()
        if tid == "":
            continue
        tax_id_to_label[tid] = get_best_tax_label(row)

    # IMPORTANT:
    # Only translate headers that are numeric AND actually exist in the taxonomy map.
    new_headers = []
    for col in original_headers:
        c = col.strip()
        if c.isdigit() and c in tax_id_to_label:
            new_headers.append(tax_id_to_label[c])
        else:
            new_headers.append(col)

    df.columns = new_headers
    df.to_csv(output_path, sep="\t", index=False)
    print(f"Output written to {output_path}")


if __name__ == "__main__":
    # Parse --version early
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--version", action="store_true")
    args, remaining = parser.parse_known_args()
    if args.version:
        print(f"translate_taxids.py version {VERSION}")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Translate tax_id columns to taxonomic names.")
    parser.add_argument("tsv_file", help="Input TSV file with tax_id headers")
    parser.add_argument("taxonomy_file", help="Taxonomy file with tax_id and taxonomic ranks")
    parser.add_argument("output_file", help="Output TSV file with translated headers")
    args = parser.parse_args(remaining)

    main(args.tsv_file, args.taxonomy_file, args.output_file)
