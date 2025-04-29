import math


def amount_dividers(n):
    amount = 0
    for i in range(2, int(math.sqrt(n)) + 1):
        amount += 1 + (n // i != n // (n // i))
    return amount


def trivial_compute(n):
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True


def advanced_compute(n, primes):
    for prime in primes:
        if n % prime == 0:
            return False
    return True
