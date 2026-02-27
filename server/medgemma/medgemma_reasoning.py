import requests
import json
import os
from glob import glob

# Configuration
URL = "https://nathan-preconversational-ardell.ngrok-free.dev/generate_report"
OUTPUT_DIR = "medgemma_reports"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FOLDERS = {
    "cough_samples": os.path.join(BASE_DIR, "..", "cough_samples"),
    "eye_images": os.path.join(BASE_DIR, "..", "eye_images"),
    "palm_captures": os.path.join(BASE_DIR, "..", "palm_captures"),
    "fingernail_images": os.path.join(BASE_DIR, "..", "fingernail_images")
}

def load_all_analysis_json():
    """Load all *_analysis.json files from the folders"""
    all_data = {}

    for folder_name, folder_path in FOLDERS.items():
        if not os.path.exists(folder_path):
            print(f"⚠️  Folder not found: {folder_path}")
            continue

        # Find all _analysis.json files
        pattern = os.path.join(folder_path, "*_analysis.json")
        json_files = glob(pattern)

        print(f"\n📁 {folder_name}: Found {len(json_files)} analysis files")

        folder_data = []
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    # Get the base name for reference
                    base_name = os.path.basename(json_file).replace("_analysis.json", "")
                    folder_data.append({
                        "file": base_name,
                        "analysis": data.get("result", {})
                    })
            except Exception as e:
                print(f"  ❌ Error reading {json_file}: {e}")

        all_data[folder_name] = folder_data

    return all_data

def generate_report(all_data):
    """Send data to MedGemma and generate report"""
    print("\n" + "="*50)
    print("Sending request to MedGemma Endpoint...")
    print("="*50)

    # Create clinical context from the data
    clinical_notes = []
    probability_scores = {}

    for folder_name, items in all_data.items():
        if items:
            clinical_notes.append(f"\n{folder_name.upper()} ANALYSIS ({len(items)} tests):")
            for item in items:
                file_name = item["file"]
                analysis = item["analysis"]

                # Extract probabilities and predictions
                if "tb_probability" in analysis:
                    prob = analysis["tb_probability"]
                    probability_scores[f"{file_name}_tb"] = prob
                    interpretation = analysis.get("interpretation", "")
                    clinical_notes.append(f"  - {file_name}: TB Probability {prob:.2%} - {interpretation}")

                elif "predictions" in analysis:
                    pred = analysis["predictions"][0] if analysis["predictions"] else {}
                    score = pred.get("triage_score", 0)
                    prediction = pred.get("prediction", "Unknown")
                    probability_scores[f"{file_name}_anemia"] = score
                    clinical_notes.append(f"  - {file_name}: {prediction} (Score: {score:.4f})")

                elif "triage_score" in analysis:
                    score = analysis["triage_score"]
                    prediction = analysis.get("prediction", "Unknown")
                    probability_scores[f"{file_name}_anemia"] = score
                    clinical_notes.append(f"  - {file_name}: {prediction} (Score: {score:.4f})")

    text_context = "Patient Diagnostic Analysis Summary:\n" + "\n".join(clinical_notes)

    data = {
        "system_prompt": "You are an expert AI radiologist. Provide a comprehensive final diagnosis report based on the clinical notes and model probability scores below. Format the report using plain text paragraphs.",
        "text_context": text_context,
        "json_scores": json.dumps(probability_scores)
    }

    try:
        response = requests.post(URL, data=data)

        if response.status_code == 200:
            print("\n✅ Success! Request processed successfully.")

            response_data = response.json()
            report_text = response_data.get("report", "")

            if not report_text.strip():
                print("❌ WARNING: The report_text received was empty!")
                return None
            else:
                # Save report with timestamp
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(OUTPUT_DIR, f"medgemma_report_{timestamp}.md")

                with open(output_file, "w", encoding="utf-8") as file:
                    file.write("# MedGemma Diagnostic Report\n\n")
                    file.write(f"Generated: {os.path.basename(output_file)}\n\n")
                    file.write("---\n\n")
                    file.write(report_text)

                print(f"📄 Report saved to: {os.path.abspath(output_file)}")
                print("\n" + "="*50)
                print("Preview of report:")
                print("="*50)
                print(report_text[:500] + "...\n")
                return output_file

        else:
            print(f"\n❌ Error! Status code: {response.status_code}")
            print(response.text)
            return None

    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

if __name__ == "__main__":
    # Load all analysis data
    all_data = load_all_analysis_json()

    if not all(all_data.values()):
        print("\n❌ No analysis data found. Please run some diagnostic tests first.")
        exit(1)

    # Generate report
    report_file = generate_report(all_data)

    if report_file:
        print(f"\n✨ Complete! Report saved at: {report_file}")
    else:
        print("\n❌ Failed to generate report.")
