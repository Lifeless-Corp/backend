import xml.etree.ElementTree as ET
import re
from datetime import datetime
from typing import Dict, List, Optional
import os


class PMCXMLParser:
    def __init__(self):
        # Focus on sections that matter for search
        self.important_sections = {
            'introduction', 'background', 'methods', 'methodology', 'materials_and_methods',
            'results', 'findings', 'discussion', 'conclusion', 'conclusions',
            'summary', 'abstract', 'objective', 'objectives'
        }

    def parse_xml_file(self, file_path: str) -> Optional[Dict]:
        """Parse single PMC XML file - optimized for search"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Extract core identifiers
            doi = self._extract_doi(root)
            pmcid = self._extract_pmcid(root)
            pmid = self._extract_pmid(root)

            # DOI is most important - skip if missing
            if not doi and not pmcid and not pmid:
                print(f"Skipping {file_path}: No valid identifiers found")
                return None

            # Extract core content
            title = self._extract_title(root)
            if not title:
                print(f"Skipping {file_path}: No title found")
                return None

            doc = {
                # Identifiers
                'doi': doi,
                'pmcid': pmcid,
                'pmid': pmid,

                # Core content
                'title': title,
                'abstract': self._extract_abstract(root),
                'full_text': self._extract_full_text(root),

                # Key metadata
                'authors': self._extract_authors(root),
                'journal': self._extract_journal(root),
                'publication_date': self._extract_publication_date(root),
                'article_type': self._extract_article_type(root),
                'keywords': self._extract_keywords(root),

                # Important sections only
                'sections': self._extract_important_sections(root),

                # Processing metadata
                'processed_at': datetime.now().isoformat()
            }

            return doc

        except Exception as e:
            print(f"Error parsing {file_path}: {str(e)}")
            return None

    def _extract_doi(self, root) -> str:
        doi_elem = root.find('.//article-id[@pub-id-type="doi"]')
        return doi_elem.text.strip() if doi_elem is not None and doi_elem.text else ""

    def _extract_pmcid(self, root) -> str:
        pmcid_elem = root.find('.//article-id[@pub-id-type="pmc"]')
        return pmcid_elem.text.strip() if pmcid_elem is not None and pmcid_elem.text else ""

    def _extract_pmid(self, root) -> str:
        pmid_elem = root.find('.//article-id[@pub-id-type="pmid"]')
        return pmid_elem.text.strip() if pmid_elem is not None and pmid_elem.text else ""

    def _extract_title(self, root) -> str:
        title_elem = root.find('.//article-title')
        if title_elem is not None:
            return self._clean_text(self._extract_text_content(title_elem))
        return ""

    def _extract_authors(self, root) -> List[Dict]:
        authors = []
        contrib_group = root.find('.//contrib-group')

        if contrib_group is not None:
            for contrib in contrib_group.findall('.//contrib[@contrib-type="author"]'):
                name_elem = contrib.find('.//name')
                if name_elem is not None:
                    given_names = name_elem.find('given-names')
                    surname = name_elem.find('surname')

                    given = given_names.text.strip() if given_names is not None and given_names.text else ""
                    sur = surname.text.strip() if surname is not None and surname.text else ""
                    full_name = f"{given} {sur}".strip()

                    if full_name:  # Only add if we have a name
                        author = {'full_name': full_name}

                        # Add ORCID if available
                        orcid_elem = contrib.find(
                            './/contrib-id[@contrib-id-type="orcid"]')
                        if orcid_elem is not None and orcid_elem.text:
                            author['orcid'] = orcid_elem.text.strip()

                        authors.append(author)

        return authors

    def _extract_journal(self, root) -> Dict:
        journal = {}

        # Journal title is essential
        journal_title = root.find('.//journal-title')
        if journal_title is not None and journal_title.text:
            journal['title'] = journal_title.text.strip()
        else:
            journal['title'] = "Unknown Journal"

        # ISSN if available
        issn = root.find('.//issn')
        if issn is not None and issn.text:
            journal['issn'] = issn.text.strip()

        return journal

    def _extract_publication_date(self, root) -> Optional[str]:
        # Try different date elements
        pub_date = (root.find('.//pub-date[@date-type="pub"]') or
                    root.find('.//pub-date[@pub-type="epub"]') or
                    root.find('.//pub-date'))

        if pub_date is not None:
            return self._parse_date(pub_date)
        return None

    def _parse_date(self, date_elem) -> Optional[str]:
        try:
            year_elem = date_elem.find('year')
            month_elem = date_elem.find('month')
            day_elem = date_elem.find('day')

            if year_elem is not None and year_elem.text:
                year = year_elem.text.strip()
                month = month_elem.text.strip() if month_elem is not None and month_elem.text else "01"
                day = day_elem.text.strip() if day_elem is not None and day_elem.text else "01"

                # Validate and format
                try:
                    month = month.zfill(2)
                    day = day.zfill(2)
                    date_str = f"{year}-{month}-{day}"

                    # Basic validation
                    from datetime import datetime
                    datetime.strptime(date_str, '%Y-%m-%d')
                    return date_str
                except:
                    return f"{year}-01-01"  # Fallback to year only

        except Exception:
            pass

        return None

    def _extract_article_type(self, root) -> str:
        article_elem = root.find('.//article')
        if article_elem is not None:
            article_type = article_elem.get('article-type', '').strip()
            return article_type if article_type else "research-article"
        return "research-article"

    def _extract_keywords(self, root) -> List[str]:
        keywords = []

        # Try different keyword locations
        for kwd_group in root.findall('.//kwd-group'):
            for kwd in kwd_group.findall('.//kwd'):
                if kwd.text and kwd.text.strip():
                    keyword = kwd.text.strip()
                    if keyword not in keywords:  # Avoid duplicates
                        keywords.append(keyword)

        return keywords

    def _extract_abstract(self, root) -> str:
        abstract_elem = root.find('.//abstract')
        if abstract_elem is not None:
            abstract_text = self._extract_text_content(abstract_elem)
            return self._clean_text(abstract_text)
        return ""

    def _extract_important_sections(self, root) -> Dict[str, str]:
        """Extract only sections that are important for search"""
        sections = {}
        body_elem = root.find('.//body')

        if body_elem is not None:
            for sec in body_elem.findall('.//sec'):
                title_elem = sec.find('.//title')
                if title_elem is not None and title_elem.text:
                    section_title = self._clean_text(title_elem.text)

                    # Normalize section title to check if it's important
                    normalized_title = re.sub(
                        r'[^a-zA-Z0-9\s]', '', section_title.lower())
                    normalized_title = re.sub(
                        r'\s+', '_', normalized_title).strip('_')

                    # Check if this section is important
                    if any(important in normalized_title for important in self.important_sections):
                        section_content = self._extract_text_content(sec)
                        if section_content:
                            # Use a clean key name
                            # Limit key length
                            section_key = normalized_title[:50]
                            sections[section_key] = self._clean_text(
                                section_content)

        return sections

    def _extract_full_text(self, root) -> str:
        """Extract complete searchable text"""
        text_parts = []

        # Add abstract
        abstract = self._extract_abstract(root)
        if abstract:
            text_parts.append(abstract)

        # Add body content
        body_elem = root.find('.//body')
        if body_elem is not None:
            body_text = self._extract_text_content(body_elem)
            if body_text:
                text_parts.append(self._clean_text(body_text))

        return " ".join(text_parts)

    def _extract_text_content(self, element) -> str:
        """Extract all text content from an element"""
        if element is None:
            return ""

        text_parts = []

        # Add element's own text
        if element.text:
            text_parts.append(element.text)

        # Recursively add children's text
        for child in element:
            child_text = self._extract_text_content(child)
            if child_text:
                text_parts.append(child_text)

            # Add tail text
            if child.tail:
                text_parts.append(child.tail)

        return " ".join(text_parts)

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""

        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove excessive punctuation
        text = re.sub(r'[.]{3,}', '...', text)

        return text.strip()
