from string import Template
import subprocess

# Need to escape $ in $$
PDF_EXIST = Template("pdftk $cmd dump_data")
#PDF_COUNT_PAGES = Template("pdftk $cmd dump_data | grep NumberOfPages | awk '{print $$2}'")

class PDFIntrospectException(Exception):
    pass

class PDFToolkit():
    """
    Helpful methodes to process PDFs
    """

    @staticmethod
    def is_pdf(file_path):
        """
        Use pdftk dump_data to check if the file we have is a PDF
        """
        command = PDF_EXIST.substitute(cmd=file_path)
        output, error = subprocess.Popen(
                            command.split(' '), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()
        if "Error" in error or "PDF header signature not found" in error:
            return False
        return True

    @staticmethod
    def count_pages(file_path):
        """
        Use pdftk dump_data to count pages
        """
        command = PDF_EXIST.substitute(cmd=file_path)
        output, error = subprocess.Popen(
                            command.split(' '), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()
 
        if error:
            raise PDFIntrospectException()
        # get nbr pages
        count_page_line = output.split('/n')[-1]
        count = count_page_line.split(':')[-1].strip()
        return count