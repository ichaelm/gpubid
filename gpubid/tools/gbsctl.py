import argparse
from urllib import request, parse, error


def main():
    parser = argparse.ArgumentParser(description='Control a GpuBid Scheduler server using this tool.')
    parser.add_argument('cmd', type=str, help='The one-liner shell command that the scheduler should run in a CUDA container.')
    parser.add_argument('--mount', type=str, help='The local path that should be mounted at /mnt/gpubid inside the container.')
    parser.add_argument('--override', action='store_true', help='Override another user\'s job if necessary')
    parser.add_argument('--scheduler', type=str, default='http://localhost:8000/run_task/', help='The URL of the scheduler server, using either HTTP or HTTPS, and including a port number.')
    args = parser.parse_args()
    task = {'cmd': args.cmd, 'priority': 1 if args.override else 0, 'mount_path': args.mount}
    data = parse.urlencode(task).encode()
    req =  request.Request(args.scheduler, data=data)
    try:
        resp = request.urlopen(req)
    except error.HTTPError as e:
        print(e)
        resp = e
    print(resp.read().decode('utf-8'))


if __name__=='__main__':
    main()
