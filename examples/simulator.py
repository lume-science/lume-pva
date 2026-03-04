from lume_pva.simulator import SimpleSimulator
import time

if __name__ == '__main__':
    sim = SimpleSimulator(pvs={
        'signal_a': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '4*sin(2*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
        'signal_b': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '2.1*sin(4.3*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
        'signal_c': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '3.3*sin(0.5544*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
    })
    
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        exit(0)
