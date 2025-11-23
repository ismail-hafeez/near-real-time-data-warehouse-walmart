"""
Script to convert Project-Report.md to PDF
Requires: markdown, weasyprint, or reportlab
"""

import os
import sys

try:
    import markdown
    from weasyprint import HTML, CSS
    from weasyprint.text.fonts import FontConfiguration
except ImportError:
    print("Installing required packages...")
    os.system(f"{sys.executable} -m pip install markdown weasyprint")
    import markdown
    from weasyprint import HTML, CSS
    from weasyprint.text.fonts import FontConfiguration

def markdown_to_pdf(md_file, pdf_file):
    """Convert markdown file to PDF"""
    
    # Read markdown file
    with open(md_file, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert markdown to HTML
    html_content = markdown.markdown(
        md_content,
        extensions=['tables', 'fenced_code', 'codehilite']
    )
    
    # Add CSS styling
    html_with_style = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{
                size: A4;
                margin: 2cm;
            }}
            body {{
                font-family: 'Times New Roman', serif;
                font-size: 11pt;
                line-height: 1.6;
                color: #333;
            }}
            h1 {{
                font-size: 18pt;
                font-weight: bold;
                margin-top: 20pt;
                margin-bottom: 10pt;
                color: #000;
                border-bottom: 2pt solid #000;
                padding-bottom: 5pt;
            }}
            h2 {{
                font-size: 14pt;
                font-weight: bold;
                margin-top: 15pt;
                margin-bottom: 8pt;
                color: #000;
            }}
            h3 {{
                font-size: 12pt;
                font-weight: bold;
                margin-top: 12pt;
                margin-bottom: 6pt;
                color: #000;
            }}
            h4 {{
                font-size: 11pt;
                font-weight: bold;
                margin-top: 10pt;
                margin-bottom: 5pt;
                color: #000;
            }}
            p {{
                margin-top: 6pt;
                margin-bottom: 6pt;
                text-align: justify;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 10pt 0;
                font-size: 10pt;
            }}
            th, td {{
                border: 1pt solid #000;
                padding: 6pt;
                text-align: left;
            }}
            th {{
                background-color: #f0f0f0;
                font-weight: bold;
            }}
            code {{
                font-family: 'Courier New', monospace;
                font-size: 9pt;
                background-color: #f5f5f5;
                padding: 2pt 4pt;
            }}
            pre {{
                background-color: #f5f5f5;
                border: 1pt solid #ddd;
                padding: 8pt;
                overflow-x: auto;
                font-size: 9pt;
            }}
            ul, ol {{
                margin-top: 6pt;
                margin-bottom: 6pt;
                padding-left: 20pt;
            }}
            li {{
                margin-top: 3pt;
                margin-bottom: 3pt;
            }}
            strong {{
                font-weight: bold;
            }}
            em {{
                font-style: italic;
            }}
        </style>
    </head>
    <body>
        {html_content}
    </body>
    </html>
    """
    
    # Convert HTML to PDF
    try:
        HTML(string=html_with_style).write_pdf(pdf_file)
        print(f"Successfully generated PDF: {pdf_file}")
    except Exception as e:
        print(f"Error generating PDF: {e}")
        print("\nTrying alternative method...")
        # Alternative: Use markdown2pdf if available
        try:
            import markdown2pdf
            markdown2pdf.convert(md_file, pdf_file)
            print(f"Successfully generated PDF using markdown2pdf: {pdf_file}")
        except:
            print("Could not generate PDF. Please install weasyprint:")
            print("pip install weasyprint")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    md_file = os.path.join(script_dir, "Project-Report.md")
    pdf_file = os.path.join(script_dir, "Project-Report.pdf")
    
    if not os.path.exists(md_file):
        print(f"Error: {md_file} not found!")
        sys.exit(1)
    
    markdown_to_pdf(md_file, pdf_file)

