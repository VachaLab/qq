# Installs qq on your current desktop and on the frontends and computing nodes
# of all Metacentrum clusters.
#
# qq is installed only into the main home directory. All other home directories
# use it from there and copy it to the scratch directory of the machine on start.

QQ_CLUSTER="metacentrum"
QQ_MAIN_HOME="/storage/brno12-cerit/home/${USER}"
QQ_HOMES=(
    # zenith
    "/storage/brno12-cerit/home/${USER}"
    # charon
    "/storage/liberec3-tul/home/${USER}"
    # elmo
    "/storage/praha5-elixir/home/${USER}"
    # nympha
    "/storage/plzen1/home/${USER}"
    # oven, perian, onyx, skirit, computing nodes (galdor, halmir, tyra, aman)
    "/storage/brno2/home/${USER}"
    # tilia
    "/storage/pruhonice1-ibot/home/${USER}"
    # computing nodes (pcr, fau, fer, mor)
    "/storage/praha2-natur/home/${USER}"
    # computing nodes (hildor)
    "/storage/budejovice1/home/${USER}"
    # computing nodes (elmu)
    "/storage/brno11-elixir/home/${USER}"
    # other storages
    "/storage/brno3-cerit/home/${USER}"
)
QQ_SHARED_BUNDLE=true
# machines with local home directories
QQ_SSH_HOSTS=(
    "samson"
)
