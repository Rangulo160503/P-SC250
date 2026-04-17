% =========================================================
% BASE DE CONOCIMIENTO - EJERCICIO 1
% =========================================================

% ---------------------------------------------------------
% PERSONAS
% persona(Nombre, Altura, ColorPelo, ColorOjos, LlevaGafas, Genero)
% ---------------------------------------------------------

persona(ana,      165, castano, cafes,   si, femenino).
persona(beatriz,  172, negro,   verdes,  si, femenino).
persona(clara,    170, rubio,   azules,  no, femenino).
persona(diana,    175, negro,   cafes,   si, femenino).
persona(elena,    168, castano, miel,    si, femenino).
persona(fabiola,  180, negro,   cafes,   no, femenino).
persona(gabriela, 169, rubio,   verdes,  si, femenino).

persona(andres,   170, rubio,   cafes,   si, masculino).
persona(bruno,    168, negro,   azules,  si, masculino).
persona(carlos,   175, castano, verdes,  si, masculino).
persona(david,    160, negro,   cafes,   no, masculino).
persona(eduardo,  178, rubio,   miel,    si, masculino).
persona(felipe,   182, castano, cafes,   si, masculino).
persona(gustavo,  171, negro,   verdes,  si, masculino).

% ---------------------------------------------------------
% PADRES
% ---------------------------------------------------------

padre(carlos, ana).
padre(carlos, bruno).
padre(carlos, andres).

padre(eduardo, clara).
padre(eduardo, david).
padre(eduardo, elena).

padre(felipe, gustavo).

% ---------------------------------------------------------
% MADRES
% ---------------------------------------------------------

madre(beatriz, ana).
madre(beatriz, bruno).
madre(beatriz, andres).

madre(diana, clara).
madre(diana, david).
madre(diana, elena).

madre(gabriela, gustavo).

% ---------------------------------------------------------
% ESPOSOS
% ---------------------------------------------------------

esposo(carlos, beatriz).
esposo(eduardo, diana).
esposo(felipe, gabriela).
esposo(gustavo, elena).

% ---------------------------------------------------------
% ESPOSAS
% ---------------------------------------------------------

esposa(beatriz, carlos).
esposa(diana, eduardo).
esposa(gabriela, felipe).
esposa(elena, gustavo).

% ---------------------------------------------------------
% REGLAS AUXILIARES DE ATRIBUTOS
% ---------------------------------------------------------

hombre(X) :-
    persona(X, _, _, _, _, masculino).

mujer(X) :-
    persona(X, _, _, _, _, femenino).

altura(X, A) :-
    persona(X, A, _, _, _, _).

color_pelo(X, P) :-
    persona(X, _, P, _, _, _).

color_ojos(X, O) :-
    persona(X, _, _, O, _, _).

lleva_gafas(X) :-
    persona(X, _, _, _, si, _).

% ---------------------------------------------------------
% REGLAS DE FAMILIA
% ---------------------------------------------------------

progenitor(P, H) :-
    padre(P, H).
progenitor(P, H) :-
    madre(P, H).

hijo(H, P) :-
    hombre(H),
    progenitor(P, H).

hija(H, P) :-
    mujer(H),
    progenitor(P, H).

hermanos(X, Y) :-
    padre(P, X), padre(P, Y),
    madre(M, X), madre(M, Y),
    X \= Y.

hermano(X, Y) :-
    hombre(X),
    hermanos(X, Y).

hermana(X, Y) :-
    mujer(X),
    hermanos(X, Y).

abuelo(A, N) :-
    hombre(A),
    progenitor(A, P),
    progenitor(P, N).

abuela(A, N) :-
    mujer(A),
    progenitor(A, P),
    progenitor(P, N).

nieto(N, A) :-
    hombre(N),
    (abuelo(A, N) ; abuela(A, N)).

nieta(N, A) :-
    mujer(N),
    (abuelo(A, N) ; abuela(A, N)).

suegro(S, X) :-
    hombre(S),
    esposo(E, X),
    padre(S, E).

suegro(S, X) :-
    hombre(S),
    esposa(X, E),
    padre(S, E).

suegra(S, X) :-
    mujer(S),
    esposo(E, X),
    madre(S, E).

suegra(S, X) :-
    mujer(S),
    esposa(X, E),
    madre(S, E).

primos(X, Y) :-
    progenitor(P1, X),
    progenitor(P2, Y),
    hermanos(P1, P2),
    X \= Y.

primo(X, Y) :-
    hombre(X),
    primos(X, Y).

prima(X, Y) :-
    mujer(X),
    primos(X, Y).

familia(X, Y) :-
    progenitor(X, Y) ;
    progenitor(Y, X) ;
    hermanos(X, Y) ;
    abuelo(X, Y) ;
    abuelo(Y, X) ;
    abuela(X, Y) ;
    abuela(Y, X) ;
    primos(X, Y) ;
    suegro(X, Y) ;
    suegro(Y, X) ;
    suegra(X, Y) ;
    suegra(Y, X).

% ---------------------------------------------------------
% REGLAS DE PAREJA
% ---------------------------------------------------------

base_pareja(Mujer, Hombre) :-
    mujer(Mujer),
    hombre(Hombre),
    Mujer \= Hombre.

% a) misma altura, mismo color de pelo, distinto color de ojos y no familia
condicion_a(Mujer, Hombre) :-
    base_pareja(Mujer, Hombre),
    altura(Mujer, A),
    altura(Hombre, A),
    color_pelo(Mujer, P),
    color_pelo(Hombre, P),
    color_ojos(Mujer, O1),
    color_ojos(Hombre, O2),
    O1 \= O2,
    \+ familia(Mujer, Hombre).

% b) distinto color de ojos, ambos con gafas, hombre más alto,
%    distinto color de pelo, y el hombre es padre o suegro de la mujer
condicion_b(Mujer, Hombre) :-
    base_pareja(Mujer, Hombre),
    color_ojos(Mujer, O1),
    color_ojos(Hombre, O2),
    O1 \= O2,
    lleva_gafas(Mujer),
    lleva_gafas(Hombre),
    altura(Hombre, AH),
    altura(Mujer, AM),
    AH > AM,
    color_pelo(Mujer, P1),
    color_pelo(Hombre, P2),
    P1 \= P2,
    (padre(Hombre, Mujer) ; suegro(Hombre, Mujer)).

% c) mismo color de pelo, mujer más alta, lleva gafas y son familia
condicion_c(Mujer, Hombre) :-
    base_pareja(Mujer, Hombre),
    color_pelo(Mujer, P),
    color_pelo(Hombre, P),
    altura(Mujer, AM),
    altura(Hombre, AH),
    AM > AH,
    lleva_gafas(Mujer),
    familia(Mujer, Hombre).

% d) padre e hija o madre e hijo, hombre más alto y con gafas
condicion_d(Mujer, Hombre) :-
    base_pareja(Mujer, Hombre),
    (
        padre(Hombre, Mujer)
        ;
        madre(Mujer, Hombre)
    ),
    altura(Hombre, AH),
    altura(Mujer, AM),
    AH > AM,
    lleva_gafas(Hombre).

% e) hermanos o primos, mismo color de ojos y la mujer
%    más alta que el padre del hombre
condicion_e(Mujer, Hombre) :-
    base_pareja(Mujer, Hombre),
    (hermanos(Mujer, Hombre) ; primos(Mujer, Hombre)),
    color_ojos(Mujer, O),
    color_ojos(Hombre, O),
    padre(PH, Hombre),
    altura(Mujer, AM),
    altura(PH, AP),
    AM > AP.

% ---------------------------------------------------------
% REGLA FINAL
% ---------------------------------------------------------

sonPareja(Mujer, Hombre) :-
    condicion_a(Mujer, Hombre).

sonPareja(Mujer, Hombre) :-
    condicion_b(Mujer, Hombre).

sonPareja(Mujer, Hombre) :-
    condicion_c(Mujer, Hombre).

sonPareja(Mujer, Hombre) :-
    condicion_d(Mujer, Hombre).

sonPareja(Mujer, Hombre) :-
    condicion_e(Mujer, Hombre).