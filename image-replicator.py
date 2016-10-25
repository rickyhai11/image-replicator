#!/usr/bin/python
# -*- coding: utf-8 -*-
"""image-replicator.py
This script will connect to keystone by using the local
glance api config file, and list all the images in the current
region.

With that list, it will check if those local images exist in
glance for other regions.  If the images do not exist, this
script will then replicate the images to those other regions.

Also of note:  This script will only work on some of the local
images.  It tries to spread the work around and only replicate
some of the images, depending upon how many control nodes exist.

So with 3 control nodes, this script will only look at 1/3 of the images
each time.

Images can be ignored if they contain the property named:
replicator.os.cloud.twc.net
"""

import argparse
import ConfigParser
import fcntl
import glanceclient as glance_client
from glanceclient import exc
from glanceclient.common import utils
from keystoneclient.v2_0 import client as keyclient
from keystoneclient.v2_0.tokens import TokenManager
import logging
import logging.handlers
import tempfile
import os
import copy
import sys
from urlparse import urlparse
import subprocess

ENDPOINT_TYPE = 'publicURL'
# the max image size to replicate
# this size is based upon our large flavor
MAX_IMAGE_SIZE = 80000000001
replicated_count = 0
already_replicated_count = 0
image_not_ready_state_count = 0
error_count = 0
shard_ignore_count = 0
too_large_count = 0


CREATE_ARGS = {"container-format": "container_format",
               "min-ram": "min_ram",
               "id": "id",
               "owner": "owner",
               "disk-format": "disk_format",
               "is-public": "is_public",
               "min-disk": "min_disk",
               "name": "name",
               "is-protected": "protected",
               }

# these are extra properties that get replicated via the
# property argument - i.e. --property os_version=os_version
EXTRA_PROPS = ["os_version", "os_distro", "ramdisk_id", "kernel_id",
               "image_type", "architecture"]


def glanceclient(url, token, version='2'):
    logging.info("Creating glance connection to the url = %s" % url)
    return glance_client.Client(version, url, token=token)


def string_hashcode(my_string):
    """will produce a hashcode integer for a string.  not a unique
    number, but pretty close to unique.
    """
    import hashlib
    return int(hashlib.md5(my_string).hexdigest(), 16)


def image_for_me(image):
    """boolean to check if this is an image this host should consider.
    this method checks the image id, and checks to mod of 3 of the
    id of the image.  this way, the image replication is split
    evenly amoungst the control nodes.
    NOTE:  this function has some hard coded garbage.
    """
    if 'replicate.os.cloud.twc.net' not in image:
        return False
    logging.info("The image %s (%s) is setup for replication."
                 % (image.name, image.id))
    # watch out, lame shard code below!
    import socket
    # grab the last 3 characters of the hostname
    machine_id = int(socket.gethostname()[-3:])
    """this part is a hack, there is no puppet or config setting with this
    info that I can see.  it will still work with more than 3 controll
    nodes - but image replication will only occur on the first 3.
    """
    total_machine_count = 3
    image_hashcode = string_hashcode(image.id)
    # the mod is going to return 0..total_machine_count and our machines
    # are 1 indexed
    if (machine_id - 1) != (image_hashcode % total_machine_count):
        logging.info("Ignoring the image %s (%s), another controller will"
                     " pick up this image" % (image.name,
                                              image.id))
        global shard_ignore_count
        shard_ignore_count += 1
        return False
    if image.size > MAX_IMAGE_SIZE:
        logging.info("The image %s (%s) is too large (%s) for replication "
                     " Ignoring." % (image.name,
                                     image.id,
                                     image.size))
        global too_large_count
        too_large_count += 1
        return False
    return True


def call_glance_image_create(args, glance_import_cmd, target_region,
                             tenant_id):
    env = {'OS_AUTH_URL': args.os_auth_url,
           'OS_USERNAME': args.os_username,
           'OS_PASSWORD': args.os_password,
           'OS_REGION_NAME': target_region,
           'OS_PROJECT_NAME': args.os_project_name,
           'OS_TENANT_NAME': args.os_project_name,
           'OS_TENANT_ID': tenant_id,
           }

    logging.info("Calling glance:  %s" % " ".join(glance_import_cmd))
    out = None
    err = None
    try:
        glance_output = subprocess.Popen(glance_import_cmd, env=env,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)
        out, err = glance_output.communicate()
        if glance_output.returncode != 0:
            raise Exception("Image upload failed return code %s" %
                            glance_output.returncode)
        logging.info("Image uploaded success!\n%s\n%s" % (out, err))
        global replicated_count
        replicated_count += 1
    except Exception:
        global error_count
        error_count += 1
        logging.exception("Image failed upload")
        if err:
            logging.error(err)
        if out:
            logging.error(out)


def get_fresh_token(args):
    # method to get a fresh token that is valid
    kwargs = {
        'auth_url': args.os_auth_url,
        'username': args.os_username,
        'password': args.os_password,
        'project_name': args.os_project_name,
    }
    keystone = keyclient.Client(**kwargs)
    auth_ref = keystone.auth_ref
    return auth_ref


def get_fresh_token_id(args):
    # method to get a fresh token id that is valid
    token = get_fresh_token(args)
    return token.service_catalog.catalog.get('token').get('id')


def image_exists(local_image, endpoint, token_id):
    try:
        # see if we can find a corresponding image in other region
        # the v1 api returns deleted images but v2 does not
        image = glanceclient(endpoint.get(ENDPOINT_TYPE),
                             token_id, version=1).images.get(local_image.id)
        global already_replicated_count
        already_replicated_count += 1
        return True
    except glance_client.exc.NotFound:
        # if the image was not found, we can continue on
        return False


def replate_images_from_region(args, other_endpoint, local_endpoint):
    # method to handle a single region
    token = get_fresh_token(args).service_catalog.catalog.get('token')
    token_id = token.get('id')
    token_tenant_id = token['tenant']['id']
    remote_images = glanceclient(other_endpoint.get(ENDPOINT_TYPE),
                                 token_id).images.list()
    for remote_image in remote_images:
        if not image_for_me(remote_image):
            continue
        if remote_image.status != 'active':
            logging.warn("Ignoring the image %s (%s) as it is not in an active"
                         " state yet" % (remote_image.name, remote_image.id))
            global image_not_ready_state_count
            image_not_ready_state_count += 1
            continue
        if image_exists(remote_image, local_endpoint,
                        get_fresh_token_id(args)):
            logging.info("The image %s (%s) is already replicated locally"
                         % (remote_image.name, remote_image.id))
            continue
        logging.info("We need to replicate the image %s (%s) to the region"
                     " %s" % (remote_image.name, remote_image.id,
                              other_endpoint.get('region')))
        # download the image
        try:
            body = glanceclient(other_endpoint.get(ENDPOINT_TYPE),
                                token_id).images.data(remote_image.id)
        except (exc.HTTPForbidden, exc.HTTPException) as e:
            msg = "Unable to download image '%s'. (%s)" % (args.id, e)
            utils.exit(msg)
        temp = tempfile.NamedTemporaryFile()
        temp_file_name = temp.name
        temp.close()
        try:
            logging.info("The image %s is downloading to %s"
                         % (remote_image.id, temp_file_name))
            utils.save_image(body, temp_file_name)
            glance_cmd = ["glance", "--os-image-api-version", "1",
                          "image-create", "--file", temp_file_name]
            # this code takes the properties on the source image, and uses them
            # for the target image
            for cli_param, glance_name in CREATE_ARGS.iteritems():
                if glance_name in remote_image:
                    value = remote_image[glance_name]
                    if " " in str(value):
                        value = "\"%s\"" % value
                    glance_cmd.extend(["--%s" % cli_param, str(value)])
            for xtra_prop in EXTRA_PROPS:
                if xtra_prop in remote_image and remote_image[xtra_prop]:
                    value = remote_image[xtra_prop]
                    if " " in str(value):
                        value = "\"%s\"" % value
                    glance_cmd.extend(["--property", "%s=%s" % (xtra_prop, value)])

            call_glance_image_create(args, glance_cmd,
                                     local_endpoint.get('region'),
                                     token_tenant_id)
        finally:
            os.remove(temp_file_name)
    logging.info('Completed processing images for the region %s' %
                 other_endpoint.get('region'))


def replicate_images(args):
    # main method which is the outter most group of code
    auth_ref = get_fresh_token(args)
    other_endpoints = []
    current_endpoint = None
    current_region = args.os_region_name
    endpoint_list = [endpoint.get('endpoints') for endpoint
                     in auth_ref.service_catalog.catalog['serviceCatalog']
                     if endpoint.get('type') == 'image']

    # essentially we have a double nested list as this point
    for endpoints in endpoint_list:
        for endpoint in endpoints:
            if current_region == endpoint.get('region'):
                current_endpoint = endpoint
            else:
                other_endpoints.append(endpoint)
    if not current_endpoint:
        logging.warn("We could not find an endpoint for the current region: %s"
                     % current_region)
        return

    for other_endpoint in other_endpoints:
        replate_images_from_region(args, other_endpoint, current_endpoint)

    logging.info('Total Images replicated= %s' % replicated_count)
    logging.info('Images already replicated= %s' % already_replicated_count)
    logging.info('Images not ready for replication= %s' %
                 image_not_ready_state_count)
    logging.info('Images replicated by others= %s' % shard_ignore_count)
    logging.info('Images too large= %s' % too_large_count)
    logging.info('Errors encountered= %s' % error_count)
    return


if __name__ == "__main__":
    lockfile = open('/tmp/glance-replicator.lock', 'w')
    try:
        fcntl.lockf(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
        print "Replicator already running, cannot obtain lock"
        sys.exit(-1)
    log_format = '%(levelname)s:%(asctime)s %(message)s'
    logging.basicConfig(level=logging.INFO,
                        format=log_format)
    LOG_FILENAME = '/var/log/glance/twc-replicator.log'
    handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                                   maxBytes=20000000,
                                                   backupCount=5)
    # create a logging format
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)

    # add the file handler to root logger
    logging.getLogger('').addHandler(handler)
    logging.getLogger('glanceclient').setLevel(logging.CRITICAL)
    parser = argparse.ArgumentParser(description='Replicate images across '
                                     'regions')
    glance_config = ConfigParser.SafeConfigParser()
    glance_config.read('/etc/glance/glance-api.conf')

    parser.add_argument('--os-auth-url',
                        default=glance_config.get('keystone_authtoken',
                                                  'auth_uri'),
                        help='Defaults to env[OS_AUTH_URL].')
    parser.add_argument('--os-region-name',
                        default=glance_config.get('glance_store',
                                                  'os_region_name'),
                        help='Defaults to env[OS_REGION_NAME].')
    parser.add_argument('--os-project-name',
                        default=glance_config.get('keystone_authtoken',
                                                  'admin_tenant_name'),
                        help='Defaults to env[OS_PROJECT_NAME].')
    parser.add_argument('--os-username',
                        default=glance_config.get('keystone_authtoken',
                                                  'admin_user'),
                        help='Defaults to env[OS_USERNAME].')
    parser.add_argument('--os-password',
                        default=glance_config.get('keystone_authtoken',
                                                  'admin_password'),
                        help='Defaults to env[OS_PASSWORD].')
    args = parser.parse_args()
    args_dict = vars(args)
    for key in args_dict:
        if not args_dict[key]:
            print "we are missing the arg %s" % key
            print parser.format_help()
            sys.exit(-1)
    replicate_images(args)
