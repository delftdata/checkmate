kubectl patch deployment ingress-nginx-controller --patch "$(cat ingress-patch.yml)" -n ingress-nginx