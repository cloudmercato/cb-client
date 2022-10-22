def stddev(pop):
    n = len(pop)
    avg = sum(pop) / n
    var = sum((x - avg)**2 for x in pop) / n
    std_dev = var ** 0.5
    return std_dev
