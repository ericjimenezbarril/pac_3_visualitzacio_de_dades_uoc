# PAC 3 — Visualització de Dades (UOC)

Projecte corresponent a la **PAC 3** de l’assignatura de **Visualització de Dades** (UOC).  
Inclou dades, codi/font i materials de suport per construir i lliurar una visualització.

## Objectiu

Construir una visualització (i la seva narrativa) a partir d’un conjunt de dades, generant els resultats finals (gràfiques / recursos / lliurable) i deixant traça del procés.

> Nota: al repositori hi ha un recurs de **Flourish** anomenat *“Hotel Booking - Cancelation Rates - Flourish.url”*, que apunta a la visualització publicada o al projecte de Flourish.

## Estructura del repositori

- data/  
  Dades d’entrada (datasets originals o pre-processats).

- src/  
  Codi font (scripts, notebooks o fitxers HTML/JS) per preparar dades i/o generar la visualització.
  NOTA: El codi de Python ha sigut creat 100% amb ChatGPT ja que l'objectiu era crear una visualizació 
  a Flourish a través de les dades output de R, i no a adaptar cada csv a cada visualització. 

- output/  
  Resultats generats: exports, gràfiques, fitxers finals o lliurables.

- theory/  
  Materials teòrics, memòria, fonts i documentació de suport.

- Hotel Booking - Cancelation Rates - Flourish.url  
  Accés directe al projecte/visualització a Flourish.

## Com utilitzar-ho

### Opció A — Veure el resultat
1. Obre la carpeta output/ i localitza el lliurable final (HTML/PDF/imatges/export).
2. Si el lliurable és un .html, obre’l amb el navegador.

### Opció B — Reproduir el procés
1. Revisa src/ per veure com es processen les dades i/o com es genera la visualització.
2. Assegura’t que les dades necessàries existeixen a data/.
3. Executa els scripts segons indiqui el propi codi (per exemple, amb Python/Node o obrint fitxers HTML locals).

## Dades i fonts

- Les dades es troben a data/.
- Les fonts, bibliografia i justificació metodològica es documenten a 	heory/.

## Llicència

Aquest repositori es distribueix sota la llicència **MIT**. Consulta el fitxer [LICENSE](LICENSE).
