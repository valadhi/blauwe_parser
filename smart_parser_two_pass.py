import os
import json
import fitz  # PyMuPDF
import pandas as pd
import time
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from google import genai
from google.genai.types import GenerateContentConfig

# --- UTILS ---
def generate_with_retry(model_name: str, contents: list, config: GenerateContentConfig, retries: int = 5, base_delay: int = 5):
    """Wraps Gemini calls with exponential backoff for 429 errors."""
    for attempt in range(retries):
        try:
            return client.models.generate_content(
                model=model_name,
                contents=contents,
                config=config
            )
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                wait_time = base_delay * (2 ** attempt) # 5s, 10s, 20s, 40s...
                print(f"   ⚠️ Quota hit (429). Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise e # Raise other errors immediately
    raise Exception("Max retries exceeded for Gemini API.")


api_key = os.getenv("GEMINI_KEY")

if not api_key:
    raise ValueError("No GEMINI_KEY found in environment variables")

client = genai.Client(api_key=api_key)


# --- GENERIC DATA STRUCTURES ---
class SampleLocation(BaseModel):
    """Defines where a specific sample is located in the document."""
    sample_id: str = Field(description="The name of the sample (e.g., 'MV27', 'Soil_1').")
    layout_type: str = Field(description="Either 'MATRIX' (columns) or 'SEQUENTIAL' (pages).")
    location_key: str = Field(
        description="If MATRIX: the column index/header (e.g., '1', '3'). If SEQUENTIAL: the page number or section title.")


class DocumentStructure(BaseModel):
    """The 'Map' of the document."""
    samples: List[SampleLocation] = Field(description="List of all samples found and their locations.")
    notes: str = Field(
        description="Any specific notes on how to interpret the tables (e.g., 'Values are in mg/kg unless stated').")


class ExtractionResult(BaseModel):
    """The universal output format."""
    sample_id: str = Field(description="The Sample ID this value belongs to.")
    parameter: str = Field(description="The name of the chemical/physical parameter (e.g., 'Lead', 'pH').")
    value: str = Field(description="The numeric value or text result (e.g., '0.5', '<0.1').")
    unit: str = Field(description="The unit of measurement (e.g., 'mg/kg', 'kg/ha').")


class PageExtraction(BaseModel):
    results: List[ExtractionResult]


# --- HELPER FUNCTIONS ---

def get_pdf_text_layout(file_path: str) -> List[str]:
    """Reads PDF and keeps page boundaries clear."""
    doc = fitz.open(file_path)
    pages = []
    for i, page in enumerate(doc):
        # We get text blocks to preserve some spatial layout (good for tables)
        text = page.get_text("text", sort=True)
        header = f"--- PAGE {i + 1} ---\n"
        pages.append(header + text)
    return pages


# --- PHASE 1: THE SCOUT (Structure Discovery) ---
def discover_structure(all_pages: List[str], base_name: str) -> DocumentStructure:
    print("🗺️  Phase 1: Scouting Document Structure...")

    full_context = "\n".join(all_pages)

    prompt = """
    You are a document layout analyst. Your job is to create a "Map" of this scientific report.

    Task:
    1. Identify all unique **Samples** being analyzed. Look for "Monsteromschrijving", "Sample ID", or "Project Name" sections.
    2. Determine if the results are presented in a **MATRIX** (columns represent samples, rows represent parameters) or **SEQUENTIAL** (one sample per page/section).
    3. If MATRIX: Identify which Column Number or Header maps to which Sample ID (e.g., "1" -> "MV27").
    4. If SEQUENTIAL: Identify which identifier signifies the start of that sample (e.g., header "14767139/MV27").

    Return the list of samples and their location keys.
    """

    try:
        response = generate_with_retry(
            model_name="gemini-2.5-flash",
            contents=[prompt, full_context],
            config=GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=DocumentStructure,
                temperature=0.0
            )
        )
        structure = response.parsed

        # [DEBUG FIX] Save the structure output
        with open(f"{base_name}_debug_structure.json", "w", encoding="utf-8") as f:
            f.write(structure.model_dump_json(indent=2))
        print(f"   🐛 Debug: Saved structure map to {base_name}_debug_structure.json")

        print(f"   -> Discovered {len(structure.samples)} samples.")
        return structure
    except Exception as e:
        print(f"   -> Discovery Failed: {e}")
        return DocumentStructure(samples=[], notes="Failed to detect.")


# --- PHASE 2: THE MINER (Context-Aware Extraction) ---
def extract_data_with_map(page_text: str, structure: DocumentStructure, base_name: str, chunk_index: int) -> List[ExtractionResult]:
    """
    Extracts data using the discovered map as context.
    """

    # We serialize the map to JSON so the LLM clearly sees the "Rules" for this specific file.
    structure_json = structure.model_dump_json()

    prompt = f"""
    You are a Scientific Data Extractor. 

    ### THE MAP (Use this to understand the layout)
    {structure_json}

    ### INSTRUCTIONS
    1. Analyze the text below.
    2. Extract chemical analysis parameters, values, and units.
    3. **CRITICAL**: Use the "Map" above to assign the correct **Sample ID** to every value.
       - If the Map says "MATRIX" and Column 1 is "MV27", then the first value in a data row belongs to "MV27".
       - If the Map says "SEQUENTIAL" and you see a header matching the location key, assign data to that sample.
    4. Ignore page numbers, footer info, and disclaimer text.
    5. Handle "less than" signs (e.g., "<0.1") as part of the value.

    ### INPUT TEXT
    {page_text}
    """

    try:
        response = generate_with_retry(
            model_name="gemini-2.5-flash",
            contents=prompt,
            config=GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=PageExtraction,
                temperature=0.0
            )
        )

        debug_filename = f"{base_name}_debug_extract_chunk_{chunk_index}.json"
        with open(debug_filename, "w", encoding="utf-8") as f:
            # We dump the parsed object, or response.text if parsing failed logic
            f.write(response.parsed.model_dump_json(indent=2))

        return response.parsed.results
    except Exception as e:
        print(f"   -> Extraction Error: {e}")
        return []


# --- MAIN PIPELINE ---

def process_generic_report(file_path: str):
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    # 1. Read File
    pages = get_pdf_text_layout(file_path)

    full_text_debug = "\n".join(pages)

    # [DEBUG FIX] Save the raw input text
    with open(f"{base_name}_debug_input.txt", "w", encoding="utf-8") as f:
        f.write(full_text_debug)
    print(f"   🐛 Debug: Saved raw input to {base_name}_debug_input.txt")

    # 2. Scout Structure (The "Map")
    structure = discover_structure(pages, base_name)

    if not structure.samples:
        print("⚠️ No samples detected. Manual review required.")
        return

    # 3. Mine Data (Chunking by logical segments)
    all_results = []

    # We assume a chunk size of 3 pages to maintain context for tables spanning pages
    chunk_size = 3
    print(f"⛏️  Phase 2: Mining Data ({len(pages)} pages)...")

    chunk_index = 0
    for i in range(0, len(pages), chunk_size):
        chunk = "\n".join(pages[i:i + chunk_size])
        chunk_index += 1
        print(f"   -> Processing pages {i + 1} to {min(i + chunk_size, len(pages))}...")

        extracted = extract_data_with_map(chunk, structure, base_name, chunk_index)
        all_results.extend(extracted)
        time.sleep(1)  # Rate limiting

    # 4. Output
    df = pd.DataFrame([vars(r) for r in all_results])

    # Cleanup: Remove potential duplicates if chunks overlapped or headers repeated
    df.drop_duplicates(subset=['sample_id', 'parameter', 'unit'], keep='last', inplace=True)

    print("\n✅ Extraction Complete.")
    print(df.head(10))

    output_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_extracted.csv"
    df.to_csv(output_name, index=False)
    print(f"Saved to {output_name}")


# --- EXECUTION ---
if __name__ == "__main__":
    # This works for BOTH Type 1 and Type 2 without changing a single line of code.
    files_to_test = [
        "certificate_2025051526_48362318.pdf"
        # "MV27_certificate_2025063802_48806662_Type2.pdf",
        # "MV27_certificate_2025063812_48808121_Type1.pdf"
    ]

    for f in files_to_test:
        if os.path.exists(f):
            print(f"\nProcessing: {f}")
            process_generic_report(f)