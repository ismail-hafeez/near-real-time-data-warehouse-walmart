"""
Simple script to convert Project-Report.md to PDF using reportlab
"""

import os
import re
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER

def parse_markdown_to_elements(md_file):
    """Parse markdown file and convert to reportlab elements"""
    with open(md_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#000000'),
        spaceAfter=12,
        borderWidth=2,
        borderColor=colors.HexColor('#000000'),
        borderPadding=5
    )
    
    heading1_style = ParagraphStyle(
        'CustomH1',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#000000'),
        spaceAfter=10,
        spaceBefore=15
    )
    
    heading2_style = ParagraphStyle(
        'CustomH2',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#000000'),
        spaceAfter=8,
        spaceBefore=12
    )
    
    heading3_style = ParagraphStyle(
        'CustomH3',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#000000'),
        spaceAfter=6,
        spaceBefore=10
    )
    
    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#333333'),
        alignment=TA_JUSTIFY,
        spaceAfter=6
    )
    
    # Split content into lines
    lines = content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            elements.append(Spacer(1, 6))
            i += 1
            continue
        
        # Headers
        if line.startswith('# '):
            text = line[2:].strip()
            elements.append(Paragraph(text, title_style))
            elements.append(Spacer(1, 12))
        elif line.startswith('## '):
            text = line[3:].strip()
            elements.append(Paragraph(text, heading1_style))
            elements.append(Spacer(1, 8))
        elif line.startswith('### '):
            text = line[4:].strip()
            elements.append(Paragraph(text, heading2_style))
            elements.append(Spacer(1, 6))
        elif line.startswith('#### '):
            text = line[5:].strip()
            elements.append(Paragraph(text, heading3_style))
            elements.append(Spacer(1, 4))
        # Tables
        elif line.startswith('|') and '---' in lines[i+1] if i+1 < len(lines) else False:
            # Parse table
            table_data = []
            j = i
            while j < len(lines) and lines[j].strip().startswith('|'):
                if '---' not in lines[j]:
                    row = [cell.strip() for cell in lines[j].strip().split('|')[1:-1]]
                    table_data.append(row)
                j += 1
            i = j - 1
            
            if table_data:
                table = Table(table_data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                ]))
                elements.append(table)
                elements.append(Spacer(1, 12))
        # Lists
        elif line.startswith('- ') or line.startswith('* '):
            text = line[2:].strip()
            # Remove markdown formatting
            text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
            text = re.sub(r'_(.*?)_', r'<i>\1</i>', text)
            elements.append(Paragraph(f"• {text}", normal_style))
        # Numbered lists
        elif re.match(r'^\d+\.\s', line):
            text = re.sub(r'^\d+\.\s', '', line)
            text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
            text = re.sub(r'_(.*?)_', r'<i>\1</i>', text)
            elements.append(Paragraph(text, normal_style))
        # Code blocks
        elif line.startswith('```'):
            # Skip code blocks for now (can be enhanced)
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                i += 1
        # Regular paragraphs
        else:
            # Clean markdown formatting
            text = line
            text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
            text = re.sub(r'_(.*?)_', r'<i>\1</i>', text)
            text = re.sub(r'`(.*?)`', r'<font name="Courier">\1</font>', text)
            
            if text.strip():
                elements.append(Paragraph(text, normal_style))
        
        i += 1
    
    return elements

def create_pdf(md_file, pdf_file):
    """Create PDF from markdown file"""
    doc = SimpleDocTemplate(
        pdf_file,
        pagesize=A4,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72
    )
    
    elements = parse_markdown_to_elements(md_file)
    doc.build(elements)
    print(f"Successfully generated PDF: {pdf_file}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    md_file = os.path.join(script_dir, "Project-Report.md")
    pdf_file = os.path.join(script_dir, "Project-Report.pdf")
    
    if not os.path.exists(md_file):
        print(f"Error: {md_file} not found!")
        exit(1)
    
    try:
        create_pdf(md_file, pdf_file)
    except ImportError:
        print("Installing reportlab...")
        os.system(f"pip install reportlab")
        create_pdf(md_file, pdf_file)

