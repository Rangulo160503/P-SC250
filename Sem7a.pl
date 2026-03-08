%HECHOS

es_hombre('Evelio').
es_hombre('Michael').
es_hombre('Jairo').
es_hombre('Gerardo').
es_hombre('Josue').
es_hombre('Arturo').
es_hombre('Mateo').
es_hombre('Luis').
es_mujer('Valeria').
es_padre('Evelio', 'Michael').
es_padre('Evelio', 'Jairo').
es_padre('Evelio', 'Gerardo').
es_padre('Michael', 'Josue').
es_padre('Michael', 'Valeria').
es_padre('Michael', 'Luis').
es_padre('Jairo', 'Arturo').
es_padre('Josue', 'Mateo').
es_madre('Sonia','Michael').
es_madre('Sonia','Jairo').
es_madre('Sonia','Gerardo').
es_madre('Veronica','Josue').
es_madre('Veronica','Valeria').
es_madre('Yanina','Arturo').
es_madre('Kristel','Mateo').
son_esposos('Evelio','Sonia').
son_esposos('Michael','Veronica').
son_esposos('Jairo','Yanina').
son_esposos('Josue','Kristel').

%REGLAS

es_hijo(A,B) :- es_padre(B,A),es_hombre(A).
es_hija(A,B) :- es_padre(B,A), es_mujer(A).

es_abuelo(E1,A) :-
    es_hombre(E1),
    es_padre(E1,Alguien),
    (es_padre(Alguien,A); es_madre(Alguien,A)).

es_abuela(Ella,A) :-
    es_mujer(Ella),
    es_madre(Ella,Alguien),
    (es_padre(Alguien,A); es_madre(Alguien,A)).

es_bisabuelo(E1,X) :-
    es_hombre(E1),
    es_padre(E1,Alguien),
    (es_abuelo(Alguien,X); es_abuela(Alguien,X)).

es_bisabuela(Ella,X) :-
    es_mujer(Ella),
    es_madre(Ella,Alguien),
    (es_abuelo(Alguien,X); es_abuela(Alguien,X)).

son_hermanos_paternos(A,B) :-
    es_hombre(C),
    es_padre(C,A),
    es_padre(C,B),
    A \== B.

son_hermanos_maternos(A,B) :-
    es_mujer(C),
    es_madre(C,A),
    es_madre(C,B),
    A \== B.

son_hermanos_matrimonio(A,B) :-
    es_padre(X,A),
    es_padre(Y,B),
    es_madre(Y,A),
    es_madre(Y,B),
    A \== B.