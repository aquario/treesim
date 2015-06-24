PAPER=paper

all:
	pdflatex $(PAPER)
	bibtex $(PAPER)
	pdflatex $(PAPER)
	pdflatex $(PAPER)

clean:
	rm -f *.dvi *.aux $(PAPER).ps $(PAPER).pdf *~ *.log *.lot *.lof *.toc *.blg *.bbl *.out
