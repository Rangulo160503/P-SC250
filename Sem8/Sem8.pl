varon('Eduardo'). 
varon('Francisco'). 
varon('Luis'). 
varon('Mario').

mujer('Alicia'). 
mujer('Veronica'). 
mujer('Victoria'). 
mujer('Beatriz').

padres('Eduardo','Francisco','Victoria'). 
padres('Alicia','Francisco','Victoria'). 
padres('Luis','Eduardo','Veronica'). 
padres('Beatriz','Mario','Alicia').

esposos('Eduardo','Veronica').
esposos('Mario','Alicia'). 
esposos('Francisco','Victoria').

hermana(Ella,X) :- mujer(Ella),padres(Ella,M,P),padres(X,M,P). 
hermano(El,X) :- varon(El),padres(El,M,P), padres(X,M,P).

hijo(El,X) :- varon(El),padres(El,X,_).
hijo(El,X) :- varon(El),padres(El,_,X).

hija(Ella,X) :- mujer(Ella),padres(Ella,X,_).
hija(Ella,X) :- mujer(Ella),padres(Ella,_,X).

abuelo(Abuelo, Nieto) :- varon(Abuelo), hijo(Padre, Abuelo), hijo(Nieto, Padre). abuelo(Abuelo, Nieto) :- varon(Abuelo), hijo(Padre, Abuelo), hija(Nieto, Padre).

abuelo(El,X) :-varon(El),padres(Y,El,_),padres(X,Y,_).
 
abuelo(El,X) :-varon(El),padres(Y,El,_),padres(X,_,Y). 