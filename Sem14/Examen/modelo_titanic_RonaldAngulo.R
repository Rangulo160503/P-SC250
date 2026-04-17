# ================================
# PASO 0 - LIMPIAR ENTORNO
# ================================

rm(list = ls())

# ================================
# PASO 1 - CARGAR DATOS
# ================================

datos <- read.csv("Datos_TitanicCP.csv", sep = ";")

# ================================
# PASO 2 - EXPLORACIÓN
# ================================

str(datos)
summary(datos)
colSums(is.na(datos))

# ================================
# PASO 3 - LIMPIEZA Y PREPROCESAMIENTO
# ================================

# Eliminar variables que no aportan
datos$ID_Pasajero <- NULL
datos$Nombre <- NULL
datos$Tiquete <- NULL
datos$Tarifa <- NULL
datos$EstadoCivil <- NULL   # 🔥 ELIMINADA

# Convertir variables categóricas
datos$Sobrevivio <- as.factor(datos$Sobrevivio)
datos$Genero <- as.factor(datos$Genero)
datos$Clase <- as.factor(datos$Clase)
datos$Puerto <- as.factor(datos$Puerto)

# Eliminar valores faltantes
datos <- na.omit(datos)

# ================================
# PASO 4 - DIVISIÓN TRAIN / TEST
# ================================

set.seed(123)

indices <- sample(1:nrow(datos), 0.85 * nrow(datos))

train <- datos[indices, ]
test <- datos[-indices, ]

# ================================
# PASO 5 - MODELO ÁRBOL (RPART)
# ================================

library(rpart)

modelo_arbol <- rpart(Sobrevivio ~ ., data = train, method = "class")

pred_arbol <- predict(modelo_arbol, test, type = "class")

# Evaluación árbol
tabla_arbol <- table(Predicho = pred_arbol, Real = test$Sobrevivio)
tabla_arbol

accuracy_arbol <- sum(diag(tabla_arbol)) / sum(tabla_arbol)
accuracy_arbol

error_arbol <- 1 - accuracy_arbol
error_arbol

# ================================
# PASO 6 - MODELO KNN
# ================================

library(kknn)

modelo_knn <- kknn(Sobrevivio ~ ., train = train, test = test, k = 5)

pred_knn <- fitted(modelo_knn)

# Evaluación KNN
tabla_knn <- table(Predicho = pred_knn, Real = test$Sobrevivio)
tabla_knn

accuracy_knn <- sum(diag(tabla_knn)) / sum(tabla_knn)
accuracy_knn

error_knn <- 1 - accuracy_knn
error_knn

# ================================
# PASO 7 - COMPARACIÓN DE MODELOS
# ================================

accuracy_arbol
accuracy_knn

# El modelo con mejor desempeño es el árbol de decisión,

# Por que presenta una mayor precisión en comparación con KNN.


# ================================
# PASO 8 - SIMULACIÓN
# ================================

mi_dato <- data.frame(
  Clase = factor(1, levels = levels(datos$Clase)),
  Genero = factor("Masculino", levels = levels(datos$Genero)),
  Edad = 25,
  Hermanos.Conyuge = 0,
  Padres.Hijos = 0,
  Puerto = factor("S", levels = levels(datos$Puerto))
)

# Predicción final
predict(modelo_arbol, mi_dato, type = "class")