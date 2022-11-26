import numpy as np
import matplotlib.pyplot as plt

dataset = "yandex"

real = []
f = open("./data/"+dataset+"_real.txt", "r")
for x in f:
  x = x.strip()
  x = float(x)
  real.append(x)

predictions = []
f = open("./data/"+dataset+"_pred.txt", "r")
for x in f:
  x = x.strip()
  x = float(x)
  predictions.append(x)
  

print(real[0:10])
print(predictions[0:10])

errors = 100 * np.abs(np.array(real) - np.array(predictions)) / np.array(predictions)
print(errors)

plt.grid(axis='y')
plt.title("Yan-TtI" + " error distribution")
plt.xlabel("prediction error percentage")
plt.ylabel("queries")
plt.hist(errors)
plt.savefig("./plots/"+dataset+"_err.png")