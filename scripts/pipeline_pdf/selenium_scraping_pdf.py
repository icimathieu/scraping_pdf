from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

# Configuration pour forcer le téléchargement des PDFs
options = Options()
options.set_preference("browser.download.folderList", 2)  # 2 = dossier personnalisé
options.set_preference("browser.download.dir", "/Users/mathieu/Downloads")  # Dossier de téléchargement (créez-le si nécessaire)
options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")  # Types MIME pour PDF
options.set_preference("pdfjs.disabled", True)  # Désactive l'ouverture des PDFs dans Firefox (force le téléchargement)

# Créez le dossier s'il n'existe pas
os.makedirs("/Users/mathieu/Documents/CODE/hackathon/vaucluse/data/pdfs/", exist_ok=True)

service = Service("/opt/homebrew/bin/geckodriver")  # Apple Silicon
driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, 10)  # Ajout du WebDriverWait

# Exemple d'URL pointant vers un PDF (remplacez par une vraie URL)
pdf_url = "https://example.com/sample.pdf"  # Remplacez par votre URL réelle

driver.get(pdf_url)
wait.until(EC.url_to_be(pdf_url))  # Attend que l'URL soit exactement celle du PDF (confirmation que la navigation est terminée)
time.sleep(2)  # Attendez que le téléchargement se termine (ajustez selon la taille)

driver.quit()

print("Téléchargement terminé. Vérifiez le dossier /Users/mathieu/Documents/CODE/hackathon/vaucluse/data/pdfs/")