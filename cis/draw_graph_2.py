import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import pickle


X_AXIS = [1,2,3,4,5]
Y1_AXIS = [1,2,3,4,5]
Y1_AXIS = [1,4,9,16,25]
Y2_AXIS = [1,8,27,64,125]

KEYS = ['xsmall', 'small', 'medium', 'large', 'xlarge']

INPUT_DIR = "results/generated-b"


def graph(x, y, xlabel, ylabel, title, output_name, legend=False):
    for value in y:
        if legend:
            plt.plot(x, value[0], label=value[1])
        else:
            plt.plot(x, value)
    plt.xscale('log')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    if legend:
        plt.legend(loc='upper left')
    plt.savefig('%s.pdf' % output_name)
    plt.clf()
    plt.cla()
    plt.close()


def get_y_values(res, alg_type, measure):
    return [res[k][alg_type][measure] for k in KEYS]


def time_graph(res, f_name):
    print 'calculate x values'
    x_axis = [res[k]['value'] for k in KEYS]
    print 'calculate y values for base'
    y_time_base = [res[k]['base']['time'] for k in KEYS]
    print 'calculate y values for spark'
    y_time_spark = [res[k]['spark']['time'] for k in KEYS] 
    print 'calculate y values for alg'
    y_time_alg = get_y_values(res, 'alg', 'time')
    graph(x_axis, [(y_time_base, "Base algorithm"), (y_time_spark, "spark algorithm"), (y_time_alg, "Our algorithm")],
          "Alpha values", "Time (secs)", "Running time as a function of alpha values", "%s.time" % f_name, legend=True)


def freq_not_identified_graph(res, f_name):
    print 'calculate x values'
    x_axis = [res[k]['value'] for k in KEYS]
    print 'calculate y values for alg'
    y_error_alg = get_y_values(res, 'alg', 'not_identified')
    graph(x_axis, [y_error_alg], "Alpha values", "Error value",
          "freq not identified as a function of alpha values", "%s.not_identified" % f_name)


def approx_overhead_graph(res, f_name):
    print 'caluclate x values'
    x_axis = [res[k]['value'] for k in KEYS]
    print 'calculate y values for alg'
    y_error_alg = get_y_values(res, 'alg', 'approx_overhead')
    graph(x_axis, [y_error_alg], "Alpha values", "Approximation overhead",
          "approximation overhead as a function of alpha values", "%s.approx_overhead" % f_name)


def mean_alpha_graph(res, f_name):
    print 'caluclate x values'
    x_axis = [res[k]['value'] for k in KEYS]
    print 'calculate y values for alg'
    y_error_alg = get_y_values(res, 'alg', 'approx_alpha_mean')
    graph(x_axis, [y_error_alg], "Alpha values", "approx_alpha_mean",
          "mean alpha as a function of alpha values", "%s.mean_alpha" % f_name)


def load_and_draw_graphs(f_name):
    res = pickle.loads(file(f_name).read())
    print 'generating time graphs'
    time_graph(res, f_name)
    print 'generating freq not identified grpahs'
    freq_not_identified_graph(res, f_name)
    print 'generating wrong cis graphs'
    approx_overhead_graph(res, f_name)
    print 'generating correct cis graphs'
    mean_alpha_graph(res, f_name)


def scan(input_dir):
    if not os.path.exists(input_dir):
        raise Exception("Input dir %s doesn't exist" % input_dir)
    if not os.path.isdir(input_dir):
        raise Exception("Path %s should specify a directory as input" % input_dir)

    for dir_name, dir_list, files_list in os.walk(input_dir):
        for f_name in files_list:
            if f_name.endswith(".res"):
                path = os.path.join(dir_name, f_name)
                print 'Handling file %s' % path
                load_and_draw_graphs(path)


def test():
    plt.plot(X_AXIS, Y1_AXIS)
    plt.plot(X_AXIS, Y2_AXIS)
    plt.xlabel("param")
    plt.ylabel("some values")
    plt.title("This is my test graph")
    plt.savefig("test.pdf")
    print 'Test done.'

if __name__ == "__main__":
    import sys
    args = sys.argv
    if len(args) != 2:
        print 'Usage: draw_graphs <input_dir>'
    input_dir = args[1]
    scan(input_dir)