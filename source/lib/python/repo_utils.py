def MakeRepoNameSafe(repo_name):
    return repo_name.replace("/", "___")


def MakeRepoNameOriginal(safe_name):
    return safe_name.replace("___", "/")
