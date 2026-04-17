% =========================================================
% EJERCICIO 2 - FUERZAS FUNDAMENTALES
% =========================================================

% ---------------------------------------------------------
% FUERZAS O INTERACCIONES
% ---------------------------------------------------------

interaccion(gravitatoria).
interaccion(electromagnetica).
interaccion(nuclear_fuerte).
interaccion(nuclear_debil).

% ---------------------------------------------------------
% COMPOSICION DE PARTICULAS
% ---------------------------------------------------------

% Quarks
quark(up).
quark(down).
quark(charm).
quark(strange).
quark(top).
quark(bottom).

% Leptones
lepton(electron).
lepton(muon).
lepton(tau).
lepton(neutrino_electronico).
lepton(neutrino_muonico).
lepton(neutrino_tauonico).

% Bosones
boson(foton).
boson(gluon).
boson(w).
boson(z).

% Fermiones = quarks + leptones
fermion(P) :-
    quark(P).

fermion(P) :-
    lepton(P).

% ---------------------------------------------------------
% PROPIEDADES
% ---------------------------------------------------------

% Neutrinos
neutrino(neutrino_electronico).
neutrino(neutrino_muonico).
neutrino(neutrino_tauonico).

% Leptones con carga electrica
% Segun el enunciado: los leptones que no son neutrinos
% son los unicos que tienen carga electrica
carga_electrica(electron).
carga_electrica(muon).
carga_electrica(tau).

% Particulas con masa
% Segun el enunciado: fotones y gluones no tienen masa
tiene_masa(P) :-
    fermion(P).

tiene_masa(w).
tiene_masa(z).

% Sin masa
sin_masa(foton).
sin_masa(gluon).

% ---------------------------------------------------------
% BOSONES NECESARIOS PARA INTERACCIONES
% ---------------------------------------------------------

boson_necesario_electromagnetica(foton).

boson_necesario_debil(w).
boson_necesario_debil(z).

% ---------------------------------------------------------
% REGLAS DE LAS 4 INTERACCIONES FUNDAMENTALES
% ---------------------------------------------------------

% La gravitatoria afecta a todas las particulas
% de manera proporcional a su masa
fuerza_gravitatoria(P) :-
    tiene_masa(P).

% La electromagnetica solo afecta a las que tengan carga electrica
% y requiere fotones
fuerza_electromagnetica(P, B) :-
    carga_electrica(P),
    boson_necesario_electromagnetica(B).

% La debil solo afecta a los neutrinos
% y requiere bosones W o Z
fuerza_debil(P, B) :-
    neutrino(P),
    boson_necesario_debil(B).

% La fuerte solo afecta a quarks y gluones
fuerza_fuerte(P) :-
    quark(P).

fuerza_fuerte(P) :-
    P = gluon.