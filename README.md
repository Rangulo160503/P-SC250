# DataSight Forecast – Sistema de análisis y predicción

## Descripción del proyecto

**DataSight Forecast** es un sistema integrado para **analizar series temporales de datos reales** (en este repositorio, información de delitos con esquema compatible con fuentes tipo OIJ), **comparar modelos predictivos**, **generar pronósticos** y **comunicar resultados** mediante una aplicación web. Resuelve un problema típico de analítica pública y de ingeniería de datos: transformar registros crudos en señales interpretables, cuantificar la incertidumbre del pronóstico y apoyar decisiones con visualizaciones y métricas coherentes.

El valor técnico del trabajo no está solo en “correr un modelo”, sino en encadenar **ingesta robusta**, **limpieza y trazabilidad**, **evaluación comparativa**, **artefactos reproducibles** y **una capa de presentación** que permite a un analista explorar EDA, tipos, relaciones, agrupaciones y pronóstico sin volver a entrenar en cada interacción.

---

## Contexto académico

Este repositorio sustenta entregables del curso **SC-250 Paradigmas de programación** (Universidad Fidélitas, Escuela de Ingeniería en Sistemas de Computación, Bachillerato en Ingeniería en Sistemas de Computación, programa cuatrimestral IIC 2024). El sílabo del curso, disponible en el PDF `SC-250 Paradigmas de programación_IIC_2024.pdf` en la raíz del repositorio, establece que la asignatura recorre fundamentos de **distintos paradigmas**, abstracción, diseño modular, **programación estructurada y orientada a objetos**, **programación lógica**, **programación funcional** (cálculo lambda, Lisp), además de un bloque explícito de **algoritmos genéticos, inteligencia artificial y machine learning** (Unidad 9).

La metodología declarada en el programa se apoya en **Aprendizaje Basado en Problemas (ABP)** y **aprendizaje activo**, orientados al análisis crítico, la resolución de problemas y la toma de decisiones. La evaluación incluye un **proyecto final programado** de alto peso (30 % del curso, con un **artículo científico** asociado del 10 % dentro de ese bloque), prácticas evaluadas, investigación y tareas. En ese marco, **DataSight Forecast** funciona como **proyecto integrador**: conecta un problema de datos reales con los objetivos del curso (pensamiento analítico, uso de varios lenguajes y paradigmas, comunicación de resultados) y sirve como pieza central de portafolio y de defensa oral.

---

## Paradigmas aplicados

| Paradigma / enfoque | Cómo se manifiesta en este repositorio |
| ------------------- | -------------------------------------- |
| **Programación estructurada e imperativa (Python)** | El pipeline en `ML1.py` y los módulos de apoyo organizan el flujo en pasos claros: lectura, normalización, agregaciones, entrenamiento, evaluación y escritura de artefactos. Es el esqueleto que garantiza mantenibilidad y depuración. |
| **Programación orientada a objetos** | Python no es “puro OO”, pero el ecosistema utilizado (por ejemplo **pandas**, **scikit-learn**, **statsmodels**, **Flask**) modela datos y comportamiento con **objetos**, **encapsulamiento** de estado en la aplicación web y separación de responsabilidades entre persistencia (`db.py`), negocio ML y presentación (`web/`). |
| **Programación lógica (Prolog)** | Las carpetas **`Sem8/`**, **`Sem9/`** y los archivos **`Sem7.pl`**, **`Sem7a.pl`** en la raíz materializan la **Unidad 7** del programa: cláusulas Horn, resolución y unificación con **Prolog**, como contraste declarativo frente al estilo imperativo del sistema principal. |
| **Programación funcional (currículo SC-250)** | El sílabo dedica la **Unidad 8** al cálculo lambda y Lisp. En este repositorio no hay un subproyecto en Lisp; sí hay **estilo funcional puntual** (composición de transformaciones, funciones puras donde conviene) dentro de scripts **R** y en el uso de APIs de alto nivel en Python. |
| **Machine learning e IA (Unidad 9)** | El núcleo **DataSight Forecast** implementa **entrenamiento**, **comparación de modelos** y **pronóstico** (series temporales con enfoques clásicos y complementos de aprendizaje automático, según `Proyecto_ML/README.md`), alineado con los contenidos de fundamentos de IA y concepto de ML del curso. |

En conjunto, el repositorio demuestra que **un mismo problema** puede abordarse con **herramientas de distinto paradigma**, que es precisamente la competencia meta que SC-250 promueve.

---

## Arquitectura del sistema

1. **Backend (Flask)** — La carpeta `Proyecto_ML/web/` concentra `main.py`, rutas, APIs para datasets y ejecución del pipeline, plantillas Jinja y estáticos (CSS/JS). Orquesta sesión, historial de corridas y descarga de artefactos.
2. **Pipeline de datos y modelos (`ML1.py`)** — Ejecutable desde línea de comandos o invocado por la app; concentra la lógica de entrenamiento, evaluación y generación de salidas.
3. **Persistencia (`db.py`, SQLite)** — Opción de almacenar datos normalizados para reutilizar el mismo flujo sin depender solo de archivos sueltos.
4. **Artefactos por ejecución** — Directorio `artifacts/<run_id>/` con pronósticos en CSV, tablas de errores, metadatos, datos limpios y gráficos HTML cuando corresponda; la UI los consume de forma **read-only** para EDA, métricas, clustering y relaciones.
5. **Visualización web** — HTML modular (plantillas y partials), hojas de estilo de producto y JavaScript para paneles, historial y gráficos interactivos (por ejemplo pronóstico con Chart.js en la vista de forecast).

La subcarpeta `Proyecto_ML/tools/` queda reservada para utilidades del proyecto; el detalle de rutas y flujos HTTP está documentado en `Proyecto_ML/README.md`.

---

## Funcionalidades principales

- **Análisis exploratorio (EDA)** — Resumen del dataset limpio, hallazgos automáticos y gráficos embebidos para calidad y comprensión del fenómeno.
- **Tipos de variables** — Inspección tipada sobre el dataset activo, con caché por ejecución.
- **Relaciones** — Detección e interpretación de relaciones entre variables; apoyo visual (incluye mapas de calor cuando el pipeline los materializa en HTML).
- **Clustering** — Segmentación y lectura interpretativa de resultados agregados (por ejemplo a nivel geográfico cuando los datos lo permiten).
- **Pronóstico (forecast)** — Comparación de modelos de series temporales, selección según métricas y visualización de trayectorias futuras (horizontes cortos y largos documentados en el README interno).
- **Dashboard final** — Vista ejecutiva que integra dataset, modelo destacado, tendencia de pronóstico y acceso a detalle técnico bajo demanda.

---

## Estructura del repositorio

| Elemento | Contenido |
| -------- | --------- |
| **`Proyecto_ML/`** | **Proyecto principal (DataSight Forecast):** código Python, app Flask, datos de trabajo y artefactos generados. Es la única copia que debe tomarse como referencia para evolución del sistema. |
| **`Sem11/Proyecto_ML/`** | Copia o iteración anterior del mismo sistema; útil como historial, no como fuente duplicada de verdad. |
| **`Datos/`** | CSV agregados por periodo (2020-2021, 2022-2023, 2024-2025) como insumo genérico o para alimentar el flujo del proyecto ML. |
| **`Nicole/`** | Caso en **R** sobre **churn** (abandono de clientes), con datos y script de caso práctico. |
| **`Sem14/`** | Prácticas en **R**, datos de churn y carpeta **`Examen/`** (incluye caso tipo Titanic, script R y datos asociados). |
| **`Sem8/`**, **`Sem9/`** | Prácticas **Prolog**. |
| **Raíz** | `Sem7.pl`, `Sem7a.pl` (Prolog), **`SC-250 Paradigmas de programación_IIC_2024.pdf`** (programa del curso), **`Proyecto_ML-master.zip`** (respaldo o distribución empaquetada del proyecto), `.gitattributes` (Git LFS). |

No se listan archivos individuales: la navegación cotidiana debe partir de estas carpetas y del README interno de `Proyecto_ML/`.

---

## Datos

Los conjuntos son principalmente **CSV** (delitos por periodo, churn, Titanic para examen). Pueden ser **grandes**; el repositorio usa **Git LFS** para `*.csv` y `*.db` según `.gitattributes`, de modo que en GitHub u otros remotos el contenido pesado viaja como objetos LFS. Tras clonar, hace falta **Git LFS** ([git-lfs.com](https://git-lfs.com/)) instalado y un `git lfs pull` si los datos aparecen como punteros. La separación **datos vs. código** reduce conflictos de merge y mantiene el foco del control de versiones en la lógica del sistema.

---

## Tecnologías

- **Python** — Pipeline, orquestación y ML.
- **Flask** — Servidor web y APIs del proyecto principal.
- **Pandas, NumPy, statsmodels, scikit-learn, Plotly, XGBoost** (según dependencias efectivas en `requirements.txt` y `ML1.py`) — Ingeniería de datos y modelado.
- **R** — Análisis estadístico y modelos en `Nicole/` y `Sem14/`.
- **Prolog** — Paradigma lógico en semanas 7–9.
- **HTML, CSS, JavaScript** — Interfaz del dashboard y experiencia de usuario.

---

## Enfoque de aprendizaje

El programa SC-250 explicita **Aprendizaje Basado en Problemas (ABP)** y aprendizaje activo. Este proyecto los materializa así:

- **Problema ancla** — Pronosticar y explicar una serie real con restricciones de datos y de software.
- **Indagación y criterio** — Comparar modelos, interpretar métricas y decidir qué comunicar a un decisor no técnico.
- **Integración** — Un mismo repositorio combina **ML**, **web**, **SQL** y **Prolog**, forzando al estudiante a **cambiar de “lente” paradigmática”** entre módulos.
- **Producto defendible** — Artefactos reproducibles, UI y documentación alineados con la rúbrica de proyecto final e **artículo científico** del curso (comunicación clara, trazabilidad de decisiones y fuentes).

---

## Autor

**Ronald Angulo**

---

## Referencias rápidas

- Programa oficial del curso: `SC-250 Paradigmas de programación_IIC_2024.pdf` (raíz del repositorio).
- Detalle técnico del sistema ML y rutas Flask: `Proyecto_ML/README.md`.
