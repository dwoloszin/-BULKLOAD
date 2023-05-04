import BULKLOAD
import timeit


print ('\nprocessing... ')
inicio = timeit.default_timer()





BULKLOAD.processArchive()



fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 

