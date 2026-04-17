# PASO #1 - CARGAR DATOS HISTORICOS
Datos_Churn <- read.csv("C:/Users/Tecnico/Downloads/Datos_Churn.csv") 
Datos_Churn <- read.csv("Datos_Churn.csv", sep = ",", stringsAsFactors = TRUE)

# PASO #2 - ELIMINAR VARIABLES DE VALOR UNICO
# Identificar columnas con un solo valor
valores_unicos <- sapply(Datos_Churn, function(x) length(unique(x)))
valores_unicos

# Eliminar columnas con un solo valor
valores_unicos <- sapply(Datos_Churn, function(x) length(unique(x)))
Datos_Churn <- Datos_Churn[, valores_unicos > 1]

# PASO #3 - ELIMINAR VALORES NULOS
Datos_Churn <- na.omit(Datos_Churn)

# Eliminación directa
Datos_Churn <- na.omit(Datos_Churn)

sum(is.na(Datos_Churn))

# PASO #4 - CODIFICAR VARIABLES
str(Datos_Churn)

# Convertir variables tipo texto a factor
Datos_Churn$Pais <- as.factor(Datos_Churn$Pais)
Datos_Churn$Genero <- as.factor(Datos_Churn$Genero)
Datos_Churn$Churn <- as.factor(Datos_Churn$Churn)

# Asegurarse que la variable objetivo sea factor

Datos_Churn$Churn <- as.factor(Datos_Churn$Churn)

# PASO #5 - DIVIDIR DATOS
set.seed(123)

tam <- dim(Datos_Churn)[1]
muestra <- sample(1:tam, round(tam * 0.20))

Pruebas <- Datos_Churn[muestra, ]
Aprendizaje <- Datos_Churn[-muestra, ]

# PASO #6 - MODELO KNN (Vecinos más cercanos)
library(kknn)

modelo_knn <- train.kknn(Churn ~ ., data = Aprendizaje, kmax = 30)

# PASO #7 - PROBAR KNN
prediccion_knn <- predict(modelo_knn, Pruebas[, colnames(Pruebas) != "Churn"])

matriz_knn <- table(Pruebas$Churn, prediccion_knn)
matriz_knn

precision_knn <- sum(diag(matriz_knn)) / sum(matriz_knn)
precision_knn

error_knn <- 1 - precision_knn
error_knn

precision_categoria_knn <- diag(matriz_knn) / rowSums(matriz_knn)
precision_categoria_knn

# PASO #8 - ARBOL DE DECISION
library(rpart)

modelo_arbol <- rpart(Churn ~ ., data = Aprendizaje)
modelo_arbol

# PASO #9 - PROBAR ARBOL
prediccion_arbol <- predict(modelo_arbol, Pruebas, type = "class")

matriz_arbol <- table(Pruebas$Churn, prediccion_arbol)
matriz_arbol

precision_arbol <- sum(diag(matriz_arbol)) / sum(matriz_arbol)
precision_arbol

error_arbol <- 1 - precision_arbol
error_arbol

precision_categoria_arbol <- diag(matriz_arbol) / rowSums(matriz_arbol)
precision_categoria_arbol

# PASO #10 - CALIBRACION (MEJORAR PRECISION)

# KNN - probar diferentes k
modelo_knn2 <- train.kknn(Churn ~ ., data = Aprendizaje, kmax = 150)
pred_knn2 <- predict(modelo_knn2, Pruebas[, colnames(Pruebas) != "Churn"])

matriz_knn2 <- table(Pruebas$Churn, pred_knn2)
precision_knn2 <- sum(diag(matriz_knn2)) / sum(matriz_knn2)
precision_knn2

# Árbol - ajustar complejidad
modelo_arbol2 <- rpart(Churn ~ ., data = Aprendizaje, control = rpart.control(cp = 0.01))
pred_arbol2 <- predict(modelo_arbol2, Pruebas, type = "class")

matriz_arbol2 <- table(Pruebas$Churn, pred_arbol2)
precision_arbol2 <- sum(diag(matriz_arbol2)) / sum(matriz_arbol2)
precision_arbol2

# PASO #11 - PREDICCION NUEVOS DATOS
Datos_ChurnNuevos <- read.csv("C:/Users/Tecnico/Downloads/Datos_ChurnNuevos.csv")

# Ajustar factores a los mismos niveles
for(col in names(Datos_ChurnNuevos)){
  if(is.factor(Aprendizaje[[col]])){
    Datos_ChurnNuevos[[col]] <- factor(Datos_ChurnNuevos[[col]], levels = levels(Aprendizaje[[col]]))
  }
}

# Predicción con el mejor modelo (elige el de mayor precisión)
pred_final <- predict(modelo_arbol2, Datos_ChurnNuevos, type = "class")

Datos_ChurnNuevos$Churn <- pred_final

Datos_ChurnNuevos