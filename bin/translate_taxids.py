#!/usr/bin/env python

import argparse
import pandas as pd
import sys

VERSION = "v0.0.2"

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
    # strip + lower for matching
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def build_row_accessor(row: pd.Series):
    # create case-insensitive access: lower(colname) -> original colname
    mapping = {str(k).strip().lower(): k for k in row.index}
    def get(colname: str):
        key = colname.strip().lower()
        if key in mapping:
            v = row[mapping[key]]
            return v
        return None
    return get

def get_best_tax_label(row: pd.Series) -> str:
    get = build_row_accessor(row)

    for rank in RANK_PRIORITY:
        # check rank itself and its aliases
        for candidate in ALIASES.get(rank, [rank]):
            v = get(candidate)
            if v is None:
                continue
            if pd.notna(v) and str(v).strip() != "":
                return str(v).strip()

    return "Unknown"

def main(tsv_path, taxonomy_path, output_path):
    df = pd.read_csv(tsv_path, sep="\t", header=0, dtype=str)
    original_headers = [str(c) for c in df.columns.tolist()]

    taxonomy_df = pd.read_csv(taxonomy_path, sep="\t", dtype=str)
    taxonomy_df = normalize_cols(taxonomy_df)

    # find tax_id column robustly
    tax_id_col = None
    for c in taxonomy_df.columns:
        if c.strip().lower() in ("tax_id", "taxid", "tax_id "):
            tax_id_col = c
            break
    if tax_id_col is None:
        raise SystemExit(f"ERROR: Could not find a tax_id column in taxonomy file. Columns: {taxonomy_df.columns.tolist()}")

    taxonomy_df = taxonomy_df.set_index(tax_id_col)

    tax_id_to_label = {
        str(tax_id).strip(): get_best_tax_label(row)
        for tax_id, row in taxonomy_df.iterrows()
    }

    new_headers = [
        tax_id_to_label.get(col.strip(), col) if col.strip().isdigit() else col
        for col in original_headers
    ]
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
