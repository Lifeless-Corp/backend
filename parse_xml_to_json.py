import xml.etree.ElementTree as ET
import json

input_file = "pubmed25n0001.xml"     # ubah sesuai nama file
output_file = "pubmed_articles.jsonl"


def parse_pubmed_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    for article in root.findall('.//PubmedArticle'):
        try:
            pmid = article.findtext('.//PMID')
            title = article.findtext('.//ArticleTitle')

            # Gabungkan semua bagian AbstractText
            abstract_elements = article.findall('.//Abstract/AbstractText')
            abstract = " ".join([el.text.strip()
                                for el in abstract_elements if el.text]) or None

            # MeSH terms
            mesh_terms = [mh.findtext('DescriptorName')
                          for mh in article.findall('.//MeshHeading')]

            # Authors
            authors = []
            for author in article.findall('.//AuthorList/Author'):
                last = author.findtext('LastName')
                initials = author.findtext('Initials')
                if last and initials:
                    authors.append(f"{last} {initials}")
                elif last:
                    authors.append(last)

            # Journal title
            journal = article.findtext('.//Journal/Title')

            # Publication types
            pub_types = [pt.text for pt in article.findall(
                './/PublicationTypeList/PublicationType') if pt.text]

            yield {
                "pmid": pmid,
                "title": title,
                "abstract": abstract,
                "mesh_terms": mesh_terms,
                "authors": authors,
                "journal": journal,
                "pub_types": pub_types
            }

        except Exception as e:
            print(f"Skipping article due to error: {e}")


# Simpan ke file JSONL
with open(output_file, 'w', encoding='utf-8') as f_out:
    for record in parse_pubmed_xml(input_file):
        json.dump(record, f_out, ensure_ascii=False)
        f_out.write('\n')
