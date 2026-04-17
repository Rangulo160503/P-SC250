%HECHOS

es_animal(perro).
es_animal(gato).
es_animal(perro).
es_animal(cocodrilo).
es_animal(perico).
es_animal(vaca).
es_arbol(pino).
es_arbol(cipres).
es_flor(rosa).
es_flor(margarita).
es_verde(cipres).
es_verde(perico).
es_verde(cocodrilo).

%REGLAS
es_vegetal(x) :- es_arbol(x).
es_vegetal(x) :- es_flor(x).
es_vegetal_verde(A) :- es_vegetal(A),es_verde(A).
