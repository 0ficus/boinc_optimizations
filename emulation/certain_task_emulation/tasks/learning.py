import numpy as np


def gradient_descent(
        X: np.ndarray,
        y: np.ndarray,
        learning_rate: float = 0.01,
        epochs: int = 10000,
) -> np.ndarray:
    X = np.column_stack([np.ones(X.shape[0]), X])
    n_features = X.shape[1]
    weights = np.random.randn(n_features)
    history = []

    for epoch in range(epochs):
        y_pred = X.dot(weights)
        error = y_pred - y
        mse = np.mean(error ** 2)
        history.append(mse)
        gradient = 2 * X.T.dot(error) / len(y)
        weights -= learning_rate * gradient

    return weights


def generate_data():
    np.random.seed(42)
    X = 2 * np.random.rand(100, 1)
    y = 4 + 3 * X + np.random.randn(100, 1)

    return X, y