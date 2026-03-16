from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "vendor_master.xlsx"


def load_vendor_master(file_path: Path = DATA_FILE) -> dict:
    if not file_path.exists():
        raise FileNotFoundError(f"Vendor master file not found: {file_path}")

    workbook = pd.read_excel(file_path, sheet_name=None)

    required_sheets = ["categories", "suppliers", "sites"]
    missing_sheets = [sheet for sheet in required_sheets if sheet not in workbook]
    if missing_sheets:
        raise ValueError(f"Missing required sheets: {missing_sheets}")

    categories = workbook["categories"].copy()
    suppliers = workbook["suppliers"].copy()
    sites = workbook["sites"].copy()

    return {
        "categories": categories,
        "suppliers": suppliers,
        "sites": sites,
    }


def print_summary(data: dict) -> None:
    print("\nVendor Master Loaded Successfully\n")
    print(f"Categories: {len(data['categories'])}")
    print(f"Suppliers : {len(data['suppliers'])}")
    print(f"Sites     : {len(data['sites'])}\n")

    print("Category Preview:")
    print(data["categories"].head(5).to_string(index=False))
    print("\nSupplier Preview:")
    print(data["suppliers"].head(5).to_string(index=False))
    print("\nSite Preview:")
    print(data["sites"].head(5).to_string(index=False))


if __name__ == "__main__":
    vendor_data = load_vendor_master()
    print_summary(vendor_data)